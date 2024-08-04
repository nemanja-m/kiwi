package kiwi.storage.bitcask;

import kiwi.common.Bytes;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.Record;

import java.io.IOException;
import java.nio.ByteBuffer;

public record ValueReference(LogSegment segment, long offset, int valueSize, long ttl,
                             long timestamp) {

    public static ValueReference of(LogSegment segment, long offset, Record record) {
        return new ValueReference(
                segment,
                offset,
                record.valueSize(),
                record.header().ttl(),
                record.header().timestamp()
        );
    }

    public Bytes get() throws IOException {
        ByteBuffer buffer = segment.read(offset, valueSize);
        return Bytes.wrap(buffer.array());
    }

    public boolean isExpired(long now) {
        return ttl > 0 && now > ttl;
    }
}
