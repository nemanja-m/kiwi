package kiwi.storage.bitcask.log;

import kiwi.common.Bytes;
import kiwi.error.KiwiReadException;
import kiwi.error.KiwiWriteException;
import kiwi.storage.bitcask.ValueReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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
    void testBaseDir() {
        LogSegment segment = LogSegment.open(root.resolve("001.log"));
        assertEquals(root, segment.baseDir());
    }

    @Test
    void testOpenAsReadOnlyThrowsOnAppend() throws IOException {
        Path file = root.resolve("001.log");
        Files.createFile(file);

        LogSegment segment = LogSegment.open(file, true);

        assertThrows(KiwiWriteException.class, () -> {
            Record record = Record.of(Bytes.wrap("k"), Bytes.wrap("v"));
            segment.append(record);
        });
    }

    @Test
    void testOpenAsAppendOnlyThrowsOnRead() {
        LogSegment segment = LogSegment.open(root.resolve("001.log"));

        Record record = Record.of(Bytes.wrap("k"), Bytes.wrap("v"));
        int written = segment.append(record);

        assertEquals(record.size(), written);
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

        Record record = Record.of(Bytes.wrap("k"), Bytes.wrap("v"));
        int written = segment.append(record);
        assertEquals(written, segment.position());
    }


    @Test
    void testEqualsTo() {
        LogSegment segment = LogSegment.open(root.resolve("000.log"));
        assertTrue(segment.equalsTo(segment));

        LogSegment other = LogSegment.open(root.resolve("001.log"));
        assertFalse(segment.equalsTo(other));

        assertFalse(segment.equalsTo(null));
    }

    @Test
    void testDirtyRatio() throws IOException {
        writeRecords(
                "001.log",
                List.of(
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v1"), 0L),
                        Record.of(Bytes.wrap("k2"), Bytes.wrap("v2"), 1L),
                        Record.of(Bytes.wrap("k3"), Bytes.wrap("v3"), 2L)
                ));
        LogSegment segment = LogSegment.open(root.resolve("001.log"), true);
        assertEquals(0.0, segment.dirtyRatio(Map.of(
                Bytes.wrap("k1"), 0L,
                Bytes.wrap("k2"), 0L,
                Bytes.wrap("k3"), 0L
        )));
        assertEquals(2.0 / 3.0, segment.dirtyRatio(Map.of(
                Bytes.wrap("k1"), 1L,
                Bytes.wrap("k2"), 1L
        )));
        assertEquals(1.0, segment.dirtyRatio(Map.of()));
    }

    @Test
    void testDirtyRatioWithTtl() throws IOException {
        writeRecords(
                "001.log",
                List.of(
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v1"), 1L),
                        Record.of(Bytes.wrap("k2"), Bytes.wrap("v2"), 1L, System.currentTimeMillis() - 1000)
                ));
        LogSegment segment = LogSegment.open(root.resolve("001.log"), true);
        assertEquals(0.5, segment.dirtyRatio(Map.of(
                Bytes.wrap("k1"), 1L,
                Bytes.wrap("k2"), 1L
        )));
    }

    @Test
    void testDirtyRatioWithEmptySegment() throws IOException {
        Files.createFile(root.resolve("001.log"));
        LogSegment emptySegment = LogSegment.open(root.resolve("001.log"), true);
        assertEquals(0.0, emptySegment.dirtyRatio(Map.of()));
    }

    @Test
    void testBuildKeyDir() throws IOException {
        writeRecords(
                "001.log",
                List.of(
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v1")),
                        Record.of(Bytes.wrap("k2"), Bytes.wrap("v2")),
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v11")),
                        Record.of(Bytes.wrap("k2"), Bytes.EMPTY),
                        Record.of(Bytes.wrap("k3"), Bytes.wrap("v3"), 0L, 1L) // Expired.
                ));

        LogSegment segment = LogSegment.open(root.resolve("001.log"), true);
        Map<Bytes, ValueReference> keydir = segment.buildKeyDir();

        assertEquals(3, keydir.size());
        assertEquals("v11", keydir.get(Bytes.wrap("k1")).get().toString());
        assertNull(keydir.get(Bytes.wrap("k2")));
        assertNull(keydir.get(Bytes.wrap("k3")));
    }

    @Test
    void testGetActiveRecordsIterator() throws IOException {
        writeRecords(
                "001.log",
                List.of(
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v1"), 0L),
                        Record.of(Bytes.wrap("k2"), Bytes.wrap("v2"), 0L),
                        Record.of(Bytes.wrap("k1"), Bytes.wrap("v11"), 1L),
                        Record.of(Bytes.wrap("k2"), Bytes.EMPTY, 1L),
                        Record.of(Bytes.wrap("k3"), Bytes.wrap("v3"), 1L, 1L) // Expired.
                ));

        LogSegment segment = LogSegment.open(root.resolve("001.log"), true);
        Map<Bytes, Long> keyTimestampMap = Map.of(
                Bytes.wrap("k1"), 1L,
                Bytes.wrap("k3"), 1L
        );

        List<Record> records = new ArrayList<>();
        for (Record record : segment.getActiveRecords(keyTimestampMap)) {
            records.add(record);
        }

        assertEquals(1, records.size());
        assertEquals(Record.of(Bytes.wrap("k1"), Bytes.wrap("v11"), 1L), records.getFirst());
    }

    private void writeRecords(String filename, List<Record> records) throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve(filename), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            records.forEach((record) -> {
                try {
                    channel.write(record.toByteBuffer());
                } catch (IOException ex) {
                    fail(ex);
                }
            });
        }
    }

}