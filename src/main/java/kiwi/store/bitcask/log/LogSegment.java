package kiwi.store.bitcask.log;

import kiwi.error.KiwiException;
import kiwi.error.KiwiReadException;
import kiwi.error.KiwiWriteException;
import kiwi.store.Utils;
import kiwi.store.bitcask.ValueReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public class LogSegment {
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

    public Map<Integer, ValueReference> buildKeydir() throws KiwiReadException {
        try {
            channel.position(0);

            Map<Integer, ValueReference> keydir = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(Record.OVERHEAD_BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();

                // Skip the checksum.
                buffer.position(buffer.position() + Long.BYTES);

                // TODO: Skip timestamp.
                long timestamp = buffer.getLong();
                int keySize = buffer.getInt();
                int valueSize = buffer.getInt();

                buffer.clear();

                ByteBuffer keyBuffer = ByteBuffer.allocate(keySize);
                channel.read(keyBuffer);

                long valuePosition = channel.position();

                // Skip the value.
                channel.position(valuePosition + valueSize);

                int keyHash = Utils.hashCode(keyBuffer.array());
                if (valueSize > 0) {
                    ValueReference valueRef = new ValueReference(this, valuePosition, valueSize);
                    keydir.put(keyHash, valueRef);
                } else {
                    // Delete records on tombstone markers.
                    keydir.put(keyHash, null);
                }
            }
            return keydir;
        } catch (IOException | IllegalStateException ex) {
            throw new KiwiReadException("Failed to build hash table from log segment " + file.toString(), ex);
        }
    }

}
