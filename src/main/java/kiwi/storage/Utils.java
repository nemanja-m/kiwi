package kiwi.storage;

import kiwi.common.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.CRC32;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

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

    public static void renameFile(Path from, Path to) {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ex) {
            logger.warn("Atomic move not supported, falling back to non-atomic move for {}", from);
            try {
                Files.move(from, to);
            } catch (IOException e) {
                logger.error("Failed to rename file {}", from, e);
            }
        } catch (IOException e) {
            logger.error("Failed to rename file {}", from, e);
        }
    }
}
