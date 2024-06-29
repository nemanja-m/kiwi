package kiwi.store.bitcask;

import java.nio.ByteBuffer;

public record Header(long checksum, long timestamp, long ttl, int keySize, int valueSize) {
    public static final int BYTES = 3 * Long.BYTES + 2 * Integer.BYTES;

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(BYTES);
        buffer.putLong(checksum);
        buffer.putLong(timestamp);
        buffer.putLong(ttl);
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.rewind();
        return buffer;
    }
}
