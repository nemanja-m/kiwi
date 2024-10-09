package kiwi.storage.bitcask;

import kiwi.common.Bytes;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BitcaskStore implements KeyValueStore<Bytes, Bytes> {
    private static final Logger logger = LoggerFactory.getLogger(BitcaskStore.class);

    private final Map<Bytes, ValueReference> keyDir;
    private LogSegment activeSegment;
    private final Clock clock;
    private final Path logDir;
    private final long logSegmentBytes;
    private final long compactionSegmentMinBytes;
    private final double minDirtyRatio;
    private final LogSegmentNameGenerator generator;

    private BitcaskStore(
            Map<Bytes, ValueReference> keyDir,
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
        this.logDir = logDir;
        this.logSegmentBytes = logSegmentBytes;
        this.compactionSegmentMinBytes = compactionSegmentMinBytes;
        this.minDirtyRatio = minDirtyRatio;
        this.generator = LogSegmentNameGenerator.from(activeSegment);

        LogCleaner logCleaner = new LogCleaner(compactionInterval);
        logCleaner.schedule(this::compactLog);
        logCleaner.schedule(this::cleanLog);
    }

    void compactLog() {
        logger.info("Log compaction started");

        Map<Bytes, Long> keyTimestampMap = buildKeyTimestampMap();

        List<LogSegment> dirtySegments = findDirtySegments(keyTimestampMap);
        if (dirtySegments.isEmpty()) {
            logger.info("No dirty segments found");
        } else {
            LogSegment newSegment = null;

            for (LogSegment dirtySegment : dirtySegments) {
                for (Record record : dirtySegment.getActiveRecords(keyTimestampMap)) {
                    if (newSegment == null || newSegment.size() >= logSegmentBytes) {
                        if (newSegment != null) {
                            newSegment.close();
                        }
                        newSegment = LogSegment.open(generator.next());
                        logger.info("Opened new compacted log segment {}", newSegment.name());
                    }

                    newSegment.append(record);

                    // Prevent keydir from being updated with stale values.
                    ValueReference currentValue = keyDir.get(record.key());
                    if (currentValue != null && currentValue.timestamp() <= record.header().timestamp()) {
                        updateKeydir(record, newSegment);
                    }
                }
            }

            if (newSegment != null) {
                newSegment.close();
            }

            for (LogSegment dirtySegment : dirtySegments) {
                dirtySegment.softDelete();
            }
        }

        logger.info("Log compaction ended");
    }

    void cleanLog() {
        logger.info("Log cleaning started");

        try (Stream<Path> paths = Files.walk(logDir)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".deleted"))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                            logger.info("Deleted log segment {}", path);
                        } catch (IOException ex) {
                            logger.warn("Failed to delete dirty segments", ex);
                        }
                    });
        } catch (IOException ex) {
            logger.warn("Failed to clean dirty segments", ex);
        }

        logger.info("Log cleaning ended");
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
                    .filter(path -> !path.equals(activeSegment.file()))
                    .sorted()
                    .map(path -> {
                        try {
                            LogSegment segment = LogSegment.open(path, true);
                            double ratio = segment.dirtyRatio(keyTimestampMap);
                            if (ratio >= minDirtyRatio) {
                                logger.info("Found segment {} with dirty ratio {}", segment.name(), String.format("%.4f", ratio));
                                return segment;
                            } else if (segment.size() < compactionSegmentMinBytes) {
                                // Delete empty or almost empty segments.
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
        return dirtySegments;
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
            updateKeydir(record, activeSegment);
        } else {
            throw new KiwiException("Failed to write to segment");
        }
        maybeRollSegment();
    }

    private void updateKeydir(Record record, LogSegment segment) {
        if (record.isTombstone()) {
            keyDir.remove(record.key());
        } else {
            long offset = segment.position() - record.valueSize();
            // TODO: Avoid creating read-only segment for each put request.
            ValueReference valueRef = ValueReference.of(segment.asReadOnly(), offset, record);
            keyDir.put(record.key(), valueRef);
        }
    }

    private void maybeRollSegment() {
        if (shouldRoll()) {
            activeSegment.close();
            activeSegment = LogSegment.open(generator.next());
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
        private Clock clock = Clock.systemUTC();
        private Map<Bytes, ValueReference> keydir;
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
                    keydir,
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

                keydir = new HashMap<>();
                for (Path segmentPath : segmentPaths) {
                    LogSegment segment = LogSegment.open(segmentPath, true);
                    keydir.putAll(segment.buildKeyDir());
                }
                // Remove tombstones.
                keydir.values().removeIf(Objects::isNull);

                // TODO: Always create new active segment file to avoid name collision during compaction process.
                Path activeSegmentPath;
                if (segmentPaths.isEmpty()) {
                    activeSegmentPath = new LogSegmentNameGenerator(logDir).next();
                } else {
                    activeSegmentPath = segmentPaths.getLast();
                }
                this.activeSegment = LogSegment.open(activeSegmentPath);
            } catch (IOException ex) {
                throw new KiwiReadException("Failed to read log directory " + logDir, ex);
            }
        }
    }

}
