package kiwi.storage;

import kiwi.common.Bytes;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Utils {
    public static long checksum(long timestamp, long ttl, Bytes key, Bytes value) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES + key.size() + value.size());
        buffer.putLong(timestamp);
        buffer.putLong(ttl);
        buffer.put(key.get());
        buffer.put(value.get());
        CRC32 crc = new CRC32();
        crc.update(buffer.array());
        return crc.getValue();
    }
}
