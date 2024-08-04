package kiwi.storage.bitcask.log;

import kiwi.common.Bytes;
import kiwi.error.KiwiException;
import kiwi.error.KiwiReadException;
import kiwi.error.KiwiWriteException;
import kiwi.storage.bitcask.Header;
import kiwi.storage.bitcask.ValueReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class LogSegment {
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final Path file;
    private final FileChannel channel;
    private final Clock clock;

    LogSegment(Path file, FileChannel channel) {
        this(file, channel, Clock.systemUTC());
    }

    LogSegment(Path file, FileChannel channel, Clock clock) {
        this.file = file;
        this.channel = channel;
        this.clock = clock;
    }

    public static LogSegment open(Path file) throws KiwiException {
        return open(file, false, Clock.systemUTC());
    }

    public static LogSegment open(Path file, boolean readOnly) throws KiwiException {
        return open(file, readOnly, Clock.systemUTC());
    }

    public static LogSegment open(Path file, boolean readOnly, Clock clock) throws KiwiException {
        try {
            FileChannel channel;
            if (readOnly) {
                channel = FileChannel.open(file, StandardOpenOption.READ);
            } else {
                channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            return new LogSegment(file, channel, clock);
        } catch (Exception ex) {
            throw new KiwiException("Failed to open log segment " + file.toString(), ex);
        }
    }

    public int append(Record record) throws KiwiWriteException {
        try {
            return channel.write(record.toByteBuffer());
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiWriteException("Failed to append record to log segment " + file.toString(), ex);
        }
    }

    public ByteBuffer read(long position, int size) throws KiwiReadException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            channel.read(buffer, position);
            return buffer;
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiReadException("Failed to read from log segment " + file.toString(), ex);
        }
    }

    public long position() throws KiwiReadException {
        try {
            return channel.position();
        } catch (IOException ex) {
            throw new KiwiReadException("Failed to get position of log segment " + file.toString(), ex);
        }
    }

    public long size() throws KiwiReadException {
        try {
            return channel.size();
        } catch (IOException ex) {
            throw new KiwiReadException("Failed to get size of log segment " + file.toString(), ex);
        }
    }

    public String name() {
        return file.getFileName().toString().replace(".log", "");
    }

    public Path baseDir() {
        return file.getParent();
    }

    public Path file() {
        return file;
    }

    public void close() {
        try {
            channel.force(true);
            channel.close();
        } catch (IOException ex) {
            logger.error("Failed to close log segment {}", file.toString(), ex);
        }
    }

    public boolean equalsTo(LogSegment other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return file.equals(other.file);
    }

    public LogSegment asReadOnly() {
        return open(file, true);
    }

    public double dirtyRatio(Map<Bytes, Long> keyTimestampMap) {
        long total = 0;
        long dirtyCount = 0;

        try {
            channel.position(0);
            ByteBuffer buffer = ByteBuffer.allocate(Header.BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();

                // Skip the checksum.
                buffer.position(buffer.position() + Long.BYTES);

                long timestamp = buffer.getLong();
                long ttl = buffer.getLong();
                int keySize = buffer.getInt();
                int valueSize = buffer.getInt();

                buffer.clear();

                if (ttl > 0 && clock.millis() > ttl) {
                    // Expired records are considered dirty.
                    dirtyCount += 1;

                    // Skip the key and value.
                    channel.position(channel.position() + keySize + valueSize);
                } else {
                    ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                    channel.read(keyBuffer);

                    Bytes key = Bytes.wrap(keyBuffer.array());

                    // Stale records are considered dirty.
                    if (!keyTimestampMap.containsKey(key) || keyTimestampMap.get(key) > timestamp) {
                        dirtyCount += 1;
                    }

                    // Skip the value.
                    channel.position(channel.position() + valueSize);
                }

                total += 1;
            }
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiReadException("Failed to calculate dirty ratio for log segment " + file, ex);
        }

        return total > 0 ? (double) dirtyCount / total : 0;
    }

    public Map<Bytes, ValueReference> buildKeyDir() throws KiwiReadException {
        logger.info("Building keydir from log segment {}", file.toString());

        try {
            channel.position(0);

            Map<Bytes, ValueReference> keyDir = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(Header.BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();

                // Skip the checksum.
                // Checksum validation is done during background compaction.
                buffer.position(buffer.position() + Long.BYTES);

                long timestamp = buffer.getLong();
                long ttl = buffer.getLong();
                int keySize = buffer.getInt();
                int valueSize = buffer.getInt();

                buffer.clear();

                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                channel.read(keyBuffer);

                long valuePosition = channel.position();

                // Skip the value.
                channel.position(valuePosition + valueSize);

                // Skip the record if TTL has expired.
                boolean expired = ttl > 0 && System.currentTimeMillis() > ttl;

                Bytes key = Bytes.wrap(keyBuffer.array());
                if (valueSize > 0 && !expired) {
                    ValueReference valueRef = new ValueReference(this, valuePosition, valueSize, ttl, timestamp);
                    keyDir.put(key, valueRef);
                } else {
                    // Skip expired records and tombstone records.
                    keyDir.put(key, null);
                }
            }
            return keyDir;
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiReadException("Failed to build hash table from log segment " + file, ex);
        }
    }

}
