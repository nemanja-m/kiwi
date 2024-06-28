package kiwi.store;

import kiwi.store.log.LogSegment;

import java.io.IOException;
import java.nio.ByteBuffer;

public record ValueMetadata(LogSegment segment, long offset, int valueSize, long timestamp) {
    public ByteBuffer read() throws IOException {
        return segment.read(offset, valueSize);
    }
}
