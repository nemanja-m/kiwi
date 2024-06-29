package kiwi.store.bitcask;

import kiwi.common.Bytes;
import kiwi.common.KeyValue;
import kiwi.store.bitcask.log.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
        store.put(Bytes.wrap("k1"), Bytes.wrap("v1"));
        store.put(Bytes.wrap("k2"), Bytes.wrap("v2"));
        store.put(Bytes.wrap("k3"), Bytes.wrap("v3"));
        store.put(Bytes.wrap("k1"), Bytes.wrap("v1-updated"));

        assertEquals(3, store.size());
        assertEquals(Bytes.wrap("v1-updated"), store.get(Bytes.wrap("k1")).orElseThrow());
        assertEquals(Bytes.wrap("v2"), store.get(Bytes.wrap("k2")).orElseThrow());
        assertEquals(Bytes.wrap("v3"), store.get(Bytes.wrap("k3")).orElseThrow());
    }

    @Test
    void testGetNonExistentKey() {
        BitcaskStore store = BitcaskStore.open(root);
        assertTrue(store.get(Bytes.wrap("k1")).isEmpty());
    }

    @Test
    void testDelete() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put(Bytes.wrap("k1"), Bytes.wrap("v1"));
        store.put(Bytes.wrap("k2"), Bytes.wrap("v2"));
        store.put(Bytes.wrap("k3"), Bytes.wrap("v3"));
        store.delete(Bytes.wrap("k1"));

        assertEquals(2, store.size());
        assertFalse(store.get(Bytes.wrap("k1")).isPresent());
        assertEquals(Bytes.wrap("v2"), store.get(Bytes.wrap("k2")).orElseThrow());
        assertEquals(Bytes.wrap("v3"), store.get(Bytes.wrap("k3")).orElseThrow());
    }

    @Test
    void testContains() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put(Bytes.wrap("k1"), Bytes.wrap("v1"));
        store.put(Bytes.wrap("k2"), Bytes.wrap("v2"));
        store.put(Bytes.wrap("k3"), Bytes.wrap("v3"));

        assertTrue(store.contains(Bytes.wrap("k1")));
        assertTrue(store.contains(Bytes.wrap("k2")));
        assertTrue(store.contains(Bytes.wrap("k3")));
        assertFalse(store.contains(Bytes.wrap("k4")));
    }

    @Test
    void testSize() {
        BitcaskStore store = BitcaskStore.open(root);
        store.put(Bytes.wrap("k1"), Bytes.wrap("v1"));
        store.put(Bytes.wrap("k2"), Bytes.wrap("v2"));
        store.put(Bytes.wrap("k3"), Bytes.wrap("v3"));
        store.delete(Bytes.wrap("k1"));

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
        assertEquals(Bytes.wrap("v1-new"), store.get(Bytes.wrap("k1")).orElseThrow());
        assertEquals(Bytes.wrap("v2-updated"), store.get(Bytes.wrap("k2")).orElseThrow());
        assertTrue(store.get(Bytes.wrap("k3")).isEmpty());
        assertEquals(Bytes.wrap("v4"), store.get(Bytes.wrap("k4")).orElseThrow());
    }

    void prepareSegment(String name, List<KeyValue<String, String>> entries) throws IOException {
        try (FileChannel channel = FileChannel.open(root.resolve(name), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            entries.forEach((entry) -> {
                try {
                    Bytes key = Bytes.wrap(entry.key());
                    Bytes value = entry.value() == null ? Record.TOMBSTONE : Bytes.wrap(entry.value());
                    channel.write(Record.of(key, value, 0).toByteBuffer());
                } catch (IOException ex) {
                    fail(ex);
                }
            });
        }
    }
}