package kiwi.store.bitcask;

import kiwi.common.Bytes;
import kiwi.error.KiwiException;
import kiwi.error.KiwiReadException;
import kiwi.store.KeyValueStore;
import kiwi.store.bitcask.log.LogSegment;
import kiwi.store.bitcask.log.LogSegmentNameGenerator;
import kiwi.store.bitcask.log.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.stream.Stream;

public class BitcaskStore implements KeyValueStore<Bytes, Bytes> {

    // TODO: Roll active segment when it reaches a certain size or time threshold

    private final Map<Bytes, ValueReference> keydir;
    private final LogSegment activeSegment;
    private final Clock clock;
    private final LogSegmentNameGenerator generator;

    BitcaskStore(
            Map<Bytes, ValueReference> keydir,
            LogSegment activeSegment,
            Clock clock) {
        this.keydir = keydir;
        this.activeSegment = activeSegment;
        this.clock = clock;
        this.generator = LogSegmentNameGenerator.from(activeSegment);
    }

    public static BitcaskStore open(Path root) {
        return rebuild(root);
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
            updateKeydir(key, value, ttl);
        } else {
            throw new KiwiException("Failed to write to segment");
        }
    }

    private void updateKeydir(Bytes key, Bytes value, long ttl) {
        if (value.equals(Record.TOMBSTONE)) {
            keydir.remove(key);
        } else {
            long offset = activeSegment.position() - value.size();
            // TODO: Avoid creating read-only segment for each put request.
            ValueReference valueRef = new ValueReference(activeSegment.asReadOnly(), offset, value.size(), ttl);
            keydir.put(key, valueRef);
        }
    }

    @Override
    public Optional<Bytes> get(Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        ValueReference valueRef = keydir.get(key);
        if (valueRef == null) {
            return Optional.empty();
        }
        if (valueRef.isExpired(clock.millis())) {
            keydir.remove(key);
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
        return keydir.containsKey(key);
    }

    @Override
    public int size() {
        return keydir.size();
    }

    static BitcaskStore rebuild(Path logDir) {
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

            Map<Bytes, ValueReference> keydir = new HashMap<>();
            for (Path segmentPath : segmentPaths) {
                LogSegment segment = LogSegment.open(segmentPath, true);
                keydir.putAll(segment.buildKeydir());
            }

            // Remove tombstones.
            keydir.values().removeIf(Objects::isNull);

            Path activeSegmentPath;
            if (segmentPaths.isEmpty()) {
                activeSegmentPath = new LogSegmentNameGenerator().next(logDir);
            } else {
                activeSegmentPath = segmentPaths.getLast();
            }
            LogSegment activeSegment = LogSegment.open(activeSegmentPath);

            return new BitcaskStore(keydir, activeSegment, Clock.systemUTC());
        } catch (IOException ex) {
            throw new KiwiReadException("Failed to read log directory " + logDir, ex);
        }
    }
}
