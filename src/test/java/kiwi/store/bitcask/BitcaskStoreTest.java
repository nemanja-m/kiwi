package kiwi.store.bitcask;

import kiwi.common.KeyValue;
import kiwi.store.bitcask.log.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BitcaskStoreTest {

    @TempDir
    Path root;

    @Test
    void testPutAndGet() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put("k1".getBytes(), "v1".getBytes());
        store.put("k2".getBytes(), "v2".getBytes());
        store.put("k3".getBytes(), "v3".getBytes());
        store.put("k1".getBytes(), "v1-updated".getBytes());

        assertEquals(3, store.size());
        assertArrayEquals("v1-updated".getBytes(), store.get("k1".getBytes()).orElseThrow());
        assertArrayEquals("v2".getBytes(), store.get("k2".getBytes()).orElseThrow());
        assertArrayEquals("v3".getBytes(), store.get("k3".getBytes()).orElseThrow());
    }

    @Test
    void testGetNonExistentKey() {
        BitcaskStore store = BitcaskStore.open(root);
        assertTrue(store.get("k1".getBytes()).isEmpty());
    }

    @Test
    void testDelete() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put("k1".getBytes(), "v1".getBytes());
        store.put("k2".getBytes(), "v2".getBytes());
        store.put("k3".getBytes(), "v3".getBytes());
        store.delete("k1".getBytes());

        assertEquals(2, store.size());
        assertTrue(store.get("k1".getBytes()).isEmpty());
        assertArrayEquals("v2".getBytes(), store.get("k2".getBytes()).orElseThrow());
        assertArrayEquals("v3".getBytes(), store.get("k3".getBytes()).orElseThrow());
    }

    @Test
    void testContains() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put("k1".getBytes(), "v1".getBytes());
        store.put("k2".getBytes(), "v2".getBytes());
        store.put("k3".getBytes(), "v3".getBytes());

        assertTrue(store.contains("k1".getBytes()));
        assertTrue(store.contains("k2".getBytes()));
        assertTrue(store.contains("k3".getBytes()));
        assertFalse(store.contains("k4".getBytes()));
    }

    @Test
    void testSize() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put("k1".getBytes(), "v1".getBytes());
        store.put("k2".getBytes(), "v2".getBytes());
        store.put("k3".getBytes(), "v3".getBytes());
        store.delete("k1".getBytes());

        assertEquals(2, store.size());
    }

    @Test
    void testRebuildPopulatesKeydir() throws IOException {
        prepareSegment("000.log", List.of(
                KeyValue.of("k1", "v1"),
                KeyValue.of("k2", "v2"),
                KeyValue.of("k3", "v3"),
                KeyValue.of("k1", null),
                KeyValue.of("k2", "v3-updated")
        ));
        prepareSegment("001.log", List.of(
                KeyValue.of("k1", "v1-new"),
                KeyValue.of("k2", "v2-updated"),
                KeyValue.of("k3", null),
                KeyValue.of("k4", "v4")
        ));
        BitcaskStore store = BitcaskStore.rebuild(root);

        assertEquals(3, store.size());
        assertArrayEquals("v1-new".getBytes(), store.get("k1".getBytes()).orElseThrow());
        assertArrayEquals("v2-updated".getBytes(), store.get("k2".getBytes()).orElseThrow());
        assertTrue(store.get("k3".getBytes()).isEmpty());
        assertArrayEquals("v4".getBytes(), store.get("k4".getBytes()).orElseThrow());
    }

    void prepareSegment(String name, List<KeyValue<String, String>> entries) throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve(name), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            entries.forEach((entry) -> {
                try {
                    ByteBuffer key = ByteBuffer.wrap(entry.key().getBytes());
                    ByteBuffer value = ByteBuffer.wrap(entry.value() == null ? Record.TOMBSTONE : entry.value().getBytes());
                    channel.write(new Record(key, value, 0).toByteBuffer());
                } catch (IOException ex) {
                    fail(ex);
                }
            });
        }
    }
}