package kiwi.store.bitcask.log;

import kiwi.common.Bytes;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Record {

    /**
     * CRC + timestamp + key size + value size.
     */
    public static final int OVERHEAD_BYTES = 2 * (Long.BYTES + Integer.BYTES);

    /**
     * Tombstone marker for deleted records.
     */
    public static final Bytes TOMBSTONE = Bytes.EMPTY;

    private final int keySize;
    private final Bytes key;
    private final int valueSize;
    private final Bytes value;
    private final long timestamp;

    public Record(Bytes key, Bytes value, long timestamp) {
        this.key = key;
        this.keySize = key.length();
        this.value = value;
        this.valueSize = value.length();
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
        buffer.put(key.get());
        buffer.put(value.get());

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), Long.BYTES, capacity() - Long.BYTES);
        long checksum = crc.getValue();

        buffer.rewind();
        buffer.putLong(checksum);
        buffer.rewind();

        return buffer;
    }
}
