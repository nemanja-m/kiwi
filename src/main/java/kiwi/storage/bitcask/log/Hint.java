package kiwi.storage.bitcask.log;

import kiwi.common.Bytes;
import kiwi.storage.bitcask.Header;

import java.nio.ByteBuffer;

public record Hint(Header header, long valuePosition, Bytes key) {
    public int size() {
        return Header.BYTES + header.keySize() + Long.BYTES;
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(size());
        buffer.put(header.toByteBuffer());
        buffer.putLong(valuePosition);
        buffer.put(key.get());
        buffer.rewind();
        return buffer;
    }
}