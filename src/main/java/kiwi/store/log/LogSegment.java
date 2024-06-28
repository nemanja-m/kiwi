package kiwi.store.log;

import kiwi.error.KiwiException;
import kiwi.store.Utils;
import kiwi.store.ValueMetadata;

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

    public static LogSegment open(Path file, boolean mutable) throws KiwiException {
        try {
            FileChannel channel;
            if (mutable) {
                channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } else {
                channel = FileChannel.open(file, StandardOpenOption.READ);
            }
            return new LogSegment(file, channel);
        } catch (Exception ex) {
            throw new KiwiException("Failed to open log segment " + file.toString(), ex);
        }
    }

    public int append(Record record) throws KiwiException {
        try {
            return channel.write(record.toByteBuffer());
        } catch (IOException ex) {
            throw new KiwiException("Failed to append record to log segment " + file.toString(), ex);
        }
    }

    public ByteBuffer read(long position, int size) throws KiwiException {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            channel.read(buffer, position);
            return buffer;
        } catch (IOException ex) {
            throw new KiwiException("Failed to read from log segment " + file.toString(), ex);
        }
    }

    public Map<Integer, ValueMetadata> hashTable() throws KiwiException {
        try {
            channel.position(0);

            Map<Integer, ValueMetadata> table = new HashMap<>();
            ByteBuffer buffer = ByteBuffer.allocate(Record.OVERHEAD_BYTES);
            while (channel.read(buffer) != -1) {
                buffer.flip();

                // Skip the checksum.
                buffer.position(buffer.position() + Long.BYTES);

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
                    ValueMetadata metadata = new ValueMetadata(this, valuePosition, valueSize, timestamp);
                    table.put(keyHash, metadata);
                } else {
                    // Delete records on tombstone markers.
                    table.remove(keyHash);
                }
            }
            return table;
        } catch (IOException ex) {
            throw new KiwiException("Failed to build hash table from log segment " + file.toString(), ex);
        }
    }

    public long position() throws KiwiException {
        try {
            return channel.position();
        } catch (IOException ex) {
            throw new KiwiException("Failed to get position of log segment " + file.toString(), ex);
        }
    }

    public String name() {
        return file.getFileName().toString();
    }

    public LogSegment readOnly() {
        return open(file, false);
    }
}
