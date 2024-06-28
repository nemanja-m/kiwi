package kiwi.store.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Record {

    /** CRC + timestamp + key size + value size. */
    public static final int OVERHEAD_BYTES = 2 * (Long.BYTES + Integer.BYTES);

    /** Tombstone marker for deleted records. */
    public static final byte[] TOMBSTONE = new byte[0];

    private final int keySize;
    private final ByteBuffer key;
    private final int valueSize;
    private final ByteBuffer value;
    private final long timestamp;

    public Record(ByteBuffer key, ByteBuffer value, long timestamp) {
        this.key = key;
        this.keySize = key.capacity();
        this.value = value;
        this.valueSize = value.capacity();
        this.timestamp = timestamp;
    }

    public int capacity() {
        return OVERHEAD_BYTES + keySize + valueSize;
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(capacity());
        buffer.putLong(0L); // Placeholder for checksum CRC32.
        buffer.putLong(timestamp);
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.put(key);
        buffer.put(value);

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), Long.BYTES, capacity() - Long.BYTES);
        long checksum = crc.getValue();

        buffer.rewind();
        buffer.putLong(checksum);
        buffer.rewind();

        return buffer;
    }
}
