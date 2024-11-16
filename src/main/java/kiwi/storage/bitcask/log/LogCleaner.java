package kiwi.storage.bitcask.log;

import kiwi.common.Bytes;
import kiwi.error.KiwiReadException;
import kiwi.storage.bitcask.KeyDir;
import kiwi.storage.bitcask.ValueReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogCleaner {
    private static final Logger logger = LoggerFactory.getLogger(LogCleaner.class);

    private static final double JITTER = 0.3;

    private final Path logDir;
    private final KeyDir keyDir;
    private final Supplier<LogSegment> activeSegmentSupplier;
    private final LogSegmentNameGenerator segmentNameGenerator;
    private final double minDirtyRatio;
    private final long compactionSegmentMinBytes;
    private final long logSegmentBytes;
    private final ScheduledExecutorService executor;

    public LogCleaner(
            Path logDir,
            KeyDir keyDir,
            Supplier<LogSegment> activeSegmentSupplier,
            LogSegmentNameGenerator segmentNameGenerator,
            double minDirtyRatio,
            long compactionSegmentMinBytes,
            long logSegmentBytes) {
        this.logDir = logDir;
        this.keyDir = keyDir;
        this.activeSegmentSupplier = activeSegmentSupplier;
        this.segmentNameGenerator = segmentNameGenerator;
        this.minDirtyRatio = minDirtyRatio;
        this.compactionSegmentMinBytes = compactionSegmentMinBytes;
        this.logSegmentBytes = logSegmentBytes;

        this.executor = Executors.newScheduledThreadPool(2);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down log cleaner");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3, TimeUnit.MINUTES)) {
                    logger.warn("Log cleaner did not shutdown in time. Forcing shutdown.");
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                logger.error("Failed to shutdown log cleaner", ex);
                executor.shutdownNow();
            }
        }));
    }

    public void start(Duration interval) {
        // Add some jitter to prevent all log cleaners from running at the same time.
        long compactIntervalSeconds = intervalWithJitterSeconds(interval);
        long cleanIntervalSeconds = intervalWithJitterSeconds(interval);

        executor.scheduleAtFixedRate(this::compactLog, compactIntervalSeconds, compactIntervalSeconds, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::cleanLog, 0, cleanIntervalSeconds, TimeUnit.SECONDS);
    }

    private long intervalWithJitterSeconds(Duration interval) {
        long jitter = (long) (interval.toSeconds() * JITTER);
        // Shift the interval by a random amount between -jitter and +jitter.
        return interval.toSeconds() + (long) ((Math.random() - 0.5) * 2 * jitter);
    }

    void compactLog() {
        logger.info("Log compaction started");

        Map<Bytes, Long> keyTimestampMap = buildKeyTimestampMap();
        List<LogSegment> dirtySegments = findDirtySegments(keyTimestampMap);

        if (dirtySegments.isEmpty()) {
            logger.info("No dirty segments found");
            return;
        }

        LogSegment newSegment = null;
        for (LogSegment dirtySegment : dirtySegments) {
            for (Record record : dirtySegment.getActiveRecords(keyTimestampMap)) {
                if (newSegment == null || newSegment.size() >= logSegmentBytes) {
                    if (newSegment != null) {
                        newSegment.close();
                    }
                    newSegment = LogSegment.open(segmentNameGenerator.next());
                    logger.info("Opened new compacted log segment {}", newSegment.name());
                }

                newSegment.append(record);

                // Prevent keydir from being updated with stale values.
                ValueReference currentValue = keyDir.get(record.key());
                if (currentValue != null && currentValue.timestamp() <= record.header().timestamp()) {
                    keyDir.update(record, newSegment);
                }
            }
        }

        if (newSegment != null) {
            newSegment.close();
        }

        // Mark dirty segments for deletion after new segments are closed.
        for (LogSegment dirtySegment : dirtySegments) {
            dirtySegment.markAsDeleted();
        }

        logger.info("Log compaction ended");
    }

    void cleanLog() {
        try (Stream<Path> paths = Files.walk(logDir)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".deleted"))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                            logger.info("Deleted marked log segment {}", path);
                        } catch (IOException ex) {
                            logger.warn("Failed to delete dirty segments", ex);
                        }
                    });
        } catch (IOException ex) {
            logger.warn("Failed to clean dirty segments", ex);
        }
    }

    private Map<Bytes, Long> buildKeyTimestampMap() {
        return keyDir.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().timestamp()));
    }

    private List<LogSegment> findDirtySegments(Map<Bytes, Long> keyTimestampMap) {
        List<LogSegment> dirtySegments = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(logDir)) {
            dirtySegments = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".log"))
                    .filter(path -> !path.equals(activeSegmentSupplier.get().file()))
                    .sorted()
                    .map(path -> {
                        try {
                            LogSegment segment = LogSegment.open(path, true);
                            double ratio = segment.dirtyRatio(keyTimestampMap);
                            if (ratio >= minDirtyRatio) {
                                logger.info("Found segment {} with dirty ratio {}", segment.name(), String.format("%.4f", ratio));
                                return segment;
                            } else if (segment.size() < compactionSegmentMinBytes) {
                                // Compact empty or almost empty segments.
                                logger.info("Found segment {} with {} bytes", segment.name(), segment.size());
                                return segment;
                            } else {
                                return null;
                            }
                        } catch (KiwiReadException ex) {
                            logger.warn("Failed to read log segment {}", path, ex);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            logger.error("Failed to mark dirty segments", ex);
        }

        // Prevents infinite compaction loop when only one dirty segment is found.
        if (dirtySegments.size() == 1 && dirtySegments.getFirst().size() < compactionSegmentMinBytes) {
            logger.info("Single dirty segment found with {} bytes. Skipping compaction.", dirtySegments.getFirst().size());
            dirtySegments.clear();
        }

        return dirtySegments;
    }
}
