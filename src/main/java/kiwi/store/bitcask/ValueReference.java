package kiwi.store.bitcask;

import kiwi.common.Bytes;
import kiwi.store.bitcask.log.LogSegment;

import java.io.IOException;
import java.nio.ByteBuffer;

public record ValueReference(LogSegment segment, long offset, int valueSize, long ttl) {
    public Bytes get() throws IOException {
        ByteBuffer buffer = segment.read(offset, valueSize);
        return Bytes.wrap(buffer.array());
    }
}
