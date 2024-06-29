package kiwi.store.bitcask;

import kiwi.store.bitcask.log.LogSegment;

import java.io.IOException;
import java.nio.ByteBuffer;

public record ValueReference(LogSegment segment, long offset, int valueSize) {
    public ByteBuffer read() throws IOException {
        return segment.read(offset, valueSize);
    }
}
