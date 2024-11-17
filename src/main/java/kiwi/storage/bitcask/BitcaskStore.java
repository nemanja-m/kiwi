package kiwi.storage.bitcask;

import kiwi.common.Bytes;
import kiwi.common.NamedThreadFactory;
import kiwi.config.Options;
import kiwi.error.KiwiException;
import kiwi.error.KiwiReadException;
import kiwi.storage.KeyValueStore;
import kiwi.storage.bitcask.log.LogCleaner;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.LogSegmentNameGenerator;
import kiwi.storage.bitcask.log.Record;
import kiwi.storage.config.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class BitcaskStore implements KeyValueStore<Bytes, Bytes> {
    private static final Logger logger = LoggerFactory.getLogger(BitcaskStore.class);

    private final KeyDir keyDir;
    private LogSegment activeSegment;
    private final Clock clock;
    private final long logSegmentBytes;
    private final LogSegmentNameGenerator segmentNameGenerator;

    private BitcaskStore(
            KeyDir keyDir,
            LogSegment activeSegment,
            Clock clock,
            Path logDir,
            long logSegmentBytes,
            long compactionSegmentMinBytes,
            Duration compactionInterval,
            double minDirtyRatio) {
        this.keyDir = keyDir;
        this.activeSegment = activeSegment;
        this.clock = clock;
        this.logSegmentBytes = logSegmentBytes;
        this.segmentNameGenerator = LogSegmentNameGenerator.from(activeSegment);

        LogCleaner logCleaner = new LogCleaner(
                logDir,
                keyDir,
                activeSegmentSupplier(),
                segmentNameGenerator,
                minDirtyRatio,
                compactionSegmentMinBytes,
                logSegmentBytes);

        logCleaner.start(compactionInterval);
    }

    public static BitcaskStore open() {
        return open(Options.defaults.storage);
    }

    public static BitcaskStore open(Path logDir) {
        return new Builder(logDir).build();
    }

    public static BitcaskStore open(StorageConfig storageConfig) {
        return new Builder(storageConfig).build();
    }

    @Override
    public void put(Bytes key, Bytes value) {
        put(key, value, 0L);
    }

    @Override
    public void put(Bytes key, Bytes value, long ttl) {
        Objects.requireNonNull(key, "key cannot be null");
        Record record = Record.of(key, value, clock.millis(), ttl);
        int written = activeSegment.append(record);
        if (written > 0) {
            keyDir.update(record, activeSegment);
        } else {
            throw new KiwiException("Failed to write to segment");
        }
        maybeRollSegment();
    }

    private void maybeRollSegment() {
        if (shouldRoll()) {
            activeSegment.markAsReadOnly();
            activeSegment = LogSegment.open(segmentNameGenerator.next());
            logger.info("Opened new log segment {}", activeSegment.name());
        }
    }

    private boolean shouldRoll() {
        return activeSegment.size() >= logSegmentBytes;
    }

    @Override
    public Optional<Bytes> get(Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        ValueReference valueRef = keyDir.get(key);
        if (valueRef == null) {
            return Optional.empty();
        }
        if (valueRef.isExpired(clock.millis())) {
            keyDir.remove(key);
            return Optional.empty();
        }
        try {
            Bytes valueBytes = valueRef.get();
            if (valueBytes.equals(Record.TOMBSTONE)) {
                return Optional.empty();
            }
            return Optional.of(valueBytes);
        } catch (IOException ex) {
            throw new KiwiException("Failed to read value from active segment " + activeSegment.name(), ex);
        }
    }

    @Override
    public void delete(Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        put(key, Record.TOMBSTONE);
    }

    @Override
    public boolean contains(Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        return keyDir.containsKey(key);
    }

    @Override
    public int size() {
        return keyDir.size();
    }

    private Supplier<LogSegment> activeSegmentSupplier() {
        return () -> activeSegment;
    }

    public static Builder Builder() {
        return new Builder();
    }

    public static Builder Builder(Path logDir) {
        return new Builder(logDir);
    }

    public static Builder Builder(StorageConfig storageConfig) {
        return new Builder(storageConfig);
    }

    public static class Builder {

        private Path logDir;
        private long logSegmentBytes;
        private long compactionSegmentMinBytes;
        private Duration compactionInterval;
        private double minDirtyRatio;
        private int threads;

        private Clock clock = Clock.systemUTC();
        private KeyDir keyDir;
        private LogSegment activeSegment;

        Builder() {
            this(Options.defaults.storage);
        }

        Builder(Path logDir) {
            this(Options.defaults.storage);
            this.logDir = logDir;
        }

        Builder(StorageConfig config) {
            this.logDir = config.log.dir;
            this.logSegmentBytes = config.log.segmentBytes;
            this.compactionSegmentMinBytes = config.log.compaction.segmentMinBytes;
            this.compactionInterval = config.log.compaction.interval;
            this.minDirtyRatio = config.log.compaction.minDirtyRatio;
            this.threads = config.log.keyDirBuilderThreads;
        }

        public Builder withLogDir(Path logDir) {
            this.logDir = logDir;
            return this;
        }

        public Builder withLogSegmentBytes(long logSegmentBytes) {
            this.logSegmentBytes = logSegmentBytes;
            return this;
        }

        public Builder withCompactionSegmentMinBytes(long minBytes) {
            this.compactionSegmentMinBytes = minBytes;
            return this;
        }

        public Builder withCompactionInterval(Duration compactionInterval) {
            this.compactionInterval = compactionInterval;
            return this;
        }

        public Builder withMinDirtyRatio(double minDirtyRatio) {
            this.minDirtyRatio = minDirtyRatio;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public BitcaskStore build() {
            init(logDir);
            return new BitcaskStore(
                    keyDir,
                    activeSegment,
                    clock,
                    logDir,
                    logSegmentBytes,
                    compactionSegmentMinBytes,
                    compactionInterval,
                    minDirtyRatio);
        }

        private void init(Path logDir) {
            try {
                Files.createDirectories(logDir);
            } catch (IOException ex) {
                throw new KiwiException("Failed to create log directory " + logDir, ex);
            }

            try (Stream<Path> paths = Files.walk(logDir)) {
                List<Path> segmentPaths = paths.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith(".log"))
                        .sorted()
                        .toList();

                keyDir = new KeyDir();

                ExecutorService executor = Executors.newFixedThreadPool(threads, new NamedThreadFactory("keydir-builder"));
                List<Future<?>> futures = new ArrayList<>();

                for (Path segmentPath : segmentPaths) {
                    futures.add(executor.submit(() -> {
                        LogSegment segment;
                        if (segmentPath.equals(segmentPaths.getLast())) {
                            activeSegment = LogSegment.open(segmentPath);
                            segment = activeSegment;
                        } else {
                            segment = LogSegment.open(segmentPath, true);
                        }

                        for (Map.Entry<Bytes, ValueReference> entry : segment.buildKeyDir().entrySet()) {
                            Bytes key = entry.getKey();
                            ValueReference value = entry.getValue();

                            // keyDir is a concurrent map, so we can safely update it without synchronization.
                            if (value == null) {
                                keyDir.remove(key);
                            } else {
                                keyDir.put(key, value);
                            }
                        }
                    }));
                }

                // Wait for all tasks to complete
                for (Future<?> future : futures) {
                    future.get();
                }
                executor.shutdown();

                // Remove tombstones.
                keyDir.values().removeIf(Objects::isNull);

                // Create new active segment when there are no segment files.
                if (activeSegment == null) {
                    Path activeSegmentPath = new LogSegmentNameGenerator(logDir).next();
                    activeSegment = LogSegment.open(activeSegmentPath);
                }
            } catch (IOException | InterruptedException | ExecutionException ex) {
                throw new KiwiReadException("Failed to read log directory " + logDir, ex);
            }
        }
    }
}
