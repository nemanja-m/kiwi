package kiwi.storage.bitcask;

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

    public static Header fromByteBuffer(ByteBuffer buffer) {
        long checksum = buffer.getLong();
        long timestamp = buffer.getLong();
        long ttl = buffer.getLong();
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        return new Header(checksum, timestamp, ttl, keySize, valueSize);
    }
}
