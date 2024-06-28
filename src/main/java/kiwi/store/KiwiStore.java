package kiwi.store;

import kiwi.error.KiwiException;
import kiwi.store.log.LogSegment;
import kiwi.store.log.LogSegmentNameGenerator;
import kiwi.store.log.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.*;
import java.util.stream.Stream;

public class KiwiStore implements KeyValueStore<byte[], byte[]> {

    // TODO: Roll active segment when it reaches a certain size or time threshold

    private final Map<Integer, ValueMetadata> table;
    private final LogSegment activeSegment;
    private final LogSegmentNameGenerator logSegmentNameGenerator;
    private final Clock clock;

    KiwiStore(
            Map<Integer, ValueMetadata> table,
            LogSegment activeSegment,
            LogSegmentNameGenerator logSegmentNameGenerator,
            Clock clock) {
        this.table = table;
        this.activeSegment = activeSegment;
        this.logSegmentNameGenerator = logSegmentNameGenerator;
        this.clock = clock;
    }

    public static KiwiStore open(String root) {
        return open(Paths.get(root));
    }

    public static KiwiStore open(Path root) {
        return rebuild(root);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        long timestamp = clock.millis();
        Record record = new Record(ByteBuffer.wrap(key), ByteBuffer.wrap(value), timestamp);
        int written = activeSegment.append(record);
        if (written > 0) {
            long offset = activeSegment.position() - value.length;
            // TODO: Avoid creating read-only segment for each put request.
            ValueMetadata metadata = new ValueMetadata(activeSegment.readOnly(), offset, value.length, timestamp);
            table.put(Utils.hashCode(key), metadata);
        } else {
            throw new KiwiException("Failed to write to segment");
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        ValueMetadata value = table.get(Utils.hashCode(key));
        if (value == null) {
            return Optional.empty();
        }
        try {
            byte[] valueBytes = value.read().array();
            if (Arrays.equals(valueBytes, Record.TOMBSTONE)) {
                return Optional.empty();
            }
            return Optional.of(valueBytes);
        } catch (IOException ex) {
            throw new KiwiException("Failed to read value from active segment " + activeSegment.name(), ex);
        }
    }

    @Override
    public void delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        put(key, Record.TOMBSTONE);
    }

    @Override
    public boolean contains(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        return table.containsKey(Utils.hashCode(key));
    }

    @Override
    public int size() {
        return table.size();
    }

    static KiwiStore rebuild(Path logDir) {
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

            Map<Integer, ValueMetadata> table = new HashMap<>();
            for (Path segmentPath : segmentPaths) {
                LogSegment segment = LogSegment.open(segmentPath, false);
                table.putAll(segment.hashTable());
            }

            Path activeSegmentPath;
            LogSegmentNameGenerator logSegmentNameGenerator;
            if (segmentPaths.isEmpty()) {
                logSegmentNameGenerator = new LogSegmentNameGenerator();
                activeSegmentPath = logSegmentNameGenerator.next(logDir);
            } else {
                activeSegmentPath = segmentPaths.getLast();
                logSegmentNameGenerator = LogSegmentNameGenerator.from(activeSegmentPath);
            }
            LogSegment activeSegment = LogSegment.open(activeSegmentPath, true);

            return new KiwiStore(table, activeSegment, logSegmentNameGenerator, Clock.systemUTC());
        } catch (IOException ex) {
            throw new KiwiException("Failed to read log directory " + logDir, ex);
        }
    }
}
