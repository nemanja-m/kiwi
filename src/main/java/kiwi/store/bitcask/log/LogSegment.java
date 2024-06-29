package kiwi.store.bitcask.log;

import kiwi.common.Bytes;
import kiwi.error.KiwiException;
import kiwi.error.KiwiReadException;
import kiwi.error.KiwiWriteException;
import kiwi.store.bitcask.Header;
import kiwi.store.bitcask.ValueReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class LogSegment {
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    private final Path file;
    private final FileChannel channel;

    public LogSegment(Path file, FileChannel channel) {
        this.file = file;
        this.channel = channel;
    }

    public static LogSegment open(Path file) throws KiwiException {
        return open(file, false);
    }

    public static LogSegment open(Path file, boolean readOnly) throws KiwiException {
        try {
            FileChannel channel;
            if (readOnly) {
                channel = FileChannel.open(file, StandardOpenOption.READ);
            } else {
                channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            return new LogSegment(file, channel);
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

    public String name() {
        return file.getFileName().toString().replace(".log", "");
    }

    public LogSegment asReadOnly() {
        return open(file, true);
    }

    public Map<Bytes, ValueReference> buildKeydir() throws KiwiReadException {
        logger.info("Building keydir from log segment {}", file.toString());

        try {
            channel.position(0);

            Map<Bytes, ValueReference> keydir = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(Header.BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();

                // Skip the checksum and timestamp.
                // Checksum validation is done during background compaction.
                buffer.position(buffer.position() + 2 * Long.BYTES);

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
                    ValueReference valueRef = new ValueReference(this, valuePosition, valueSize, ttl);
                    keydir.put(key, valueRef);
                } else {
                    // Skip expired records and tombstone records.
                    keydir.put(key, null);
                }
            }
            return keydir;
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiReadException("Failed to build hash table from log segment " + file, ex);
        }
    }

}
