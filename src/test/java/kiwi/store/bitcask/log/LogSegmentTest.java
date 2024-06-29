package kiwi.store.bitcask.log;

import kiwi.common.Bytes;
import kiwi.error.KiwiReadException;
import kiwi.error.KiwiWriteException;
import kiwi.store.bitcask.ValueReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class LogSegmentTest {

    @TempDir
    Path root;

    @Test
    void testName() {
        LogSegment segment = LogSegment.open(root.resolve("001.log"));
        assertEquals("001", segment.name());
    }

    @Test
    void testOpenAsReadOnlyThrowsOnAppend() throws IOException {
        Path file = root.resolve("001.log");
        Files.createFile(file);

        LogSegment segment = LogSegment.open(file, true);

        assertThrows(KiwiWriteException.class, () -> {
            Record record = new Record(Bytes.wrap("k"), Bytes.wrap("v"), 0);
            segment.append(record);
        });
    }

    @Test
    void testOpenAsAppendOnlyThrowsOnRead() {
        LogSegment segment = LogSegment.open(root.resolve("001.log"));

        Record record = new Record(Bytes.wrap("k"), Bytes.wrap("v"), 0);
        int written = segment.append(record);

        assertEquals(Record.OVERHEAD_BYTES + 2, written);

        assertThrows(KiwiReadException.class, () -> segment.read(0, 1));
    }

    @Test
    void testOpenAsAppendOnlyCreatesSegmentFile() {
        LogSegment.open(root.resolve("001.log"));
        assertTrue(Files.exists(root.resolve("001.log")));
    }

    @Test
    void testPosition() {
        LogSegment segment = LogSegment.open(root.resolve("001.log"));
        assertEquals(0, segment.position());

        Record record = new Record(Bytes.wrap("k"), Bytes.wrap("v"), 0);
        int written = segment.append(record);
        assertEquals(written, segment.position());
    }

    @Test
    void testBuildKeydir() throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve("001.log"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            List.of(
                    new Record(Bytes.wrap("k1"), Bytes.wrap("v1"), 0),
                    new Record(Bytes.wrap("k2"), Bytes.wrap("v2"), 0),
                    new Record(Bytes.wrap("k1"), Bytes.wrap("v11"), 0),
                    new Record(Bytes.wrap("k2"), Bytes.EMPTY, 0)
            ).forEach((record) -> {
                try {
                    channel.write(record.toByteBuffer());
                } catch (IOException ex) {
                    fail(ex);
                }
            });
        }

        LogSegment segment = LogSegment.open(root.resolve("001.log"), true);
        Map<Bytes, ValueReference> keydir = segment.buildKeydir();

        assertEquals(2, keydir.size());
        assertEquals("v11", keydir.get(Bytes.wrap("k1")).get().toString());
        assertNull(keydir.get(Bytes.wrap("k2")));
    }

}