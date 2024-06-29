package kiwi.store.bitcask.log;

import kiwi.common.Bytes;
import kiwi.store.Utils;
import kiwi.store.bitcask.Header;

import java.nio.ByteBuffer;

public class Record {

    /**
     * Tombstone marker for deleted records.
     */
    public static final Bytes TOMBSTONE = Bytes.EMPTY;

    private final Header header;
    private final Bytes key;
    private final Bytes value;

    public Record(Header header, Bytes key, Bytes value) {
        this.header = header;
        this.key = key;
        this.value = value;
    }

    public static Record of(Bytes key, Bytes value) {
        return Record.of(key, value, 0L, 0L);
    }

    public static Record of(Bytes key, Bytes value, long timestamp) {
        return Record.of(key, value, timestamp, 0L);
    }

    public static Record of(Bytes key, Bytes value, long timestamp, long ttl) {
        long checksum = Utils.checksum(timestamp, ttl, key, value);
        Header header = new Header(checksum, timestamp, ttl, key.size(), value.size());
        return new Record(header, key, value);
    }

    public int size() {
        return Header.BYTES + key.size() + value.size();
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(size());
        buffer.put(header.toByteBuffer());
        buffer.put(key.get());
        buffer.put(value.get());
        buffer.rewind();
        return buffer;
    }

    public boolean isValidChecksum() {
        return header.checksum() == Utils.checksum(header.timestamp(), header.ttl(), key, value);
    }
}
