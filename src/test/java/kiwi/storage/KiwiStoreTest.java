package kiwi.storage;

import kiwi.common.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KiwiStoreTest {

    @TempDir
    Path root;

    private KiwiStore store;

    @BeforeEach
    void setUp() {
        store = KiwiStore.open(root);
    }

    @Test
    void testPutAndGet() {
        Bytes key = Bytes.wrap("key");
        Bytes value = Bytes.wrap("value");

        store.put(key, value);

        Optional<Bytes> retrievedValue = store.get(key);
        assertEquals(value, retrievedValue.orElseThrow());
        assertEquals(1, store.size());
    }

    @Test
    void testPutAndGetWithTTL() {
        Bytes key = Bytes.wrap("key");
        Bytes value = Bytes.wrap("value");

        store.put(key, value, System.currentTimeMillis() - 30 * 1000);

        assertEquals(1, store.size());
        Optional<Bytes> retrievedValue = store.get(key);
        assertTrue(retrievedValue.isEmpty());
        assertEquals(0, store.size());
    }

    @Test
    void testGetNonExistentKey() {
        Bytes key = Bytes.wrap("key");
        Optional<Bytes> retrievedValue = store.get(key);
        assertTrue(retrievedValue.isEmpty());
    }

    @Test
    void testDelete() {
        Bytes key = Bytes.wrap("key");
        Bytes value = Bytes.wrap("value");

        store.put(key, value);
        store.delete(key);

        Optional<Bytes> retrievedValue = store.get(key);
        assertTrue(retrievedValue.isEmpty());
        assertEquals(0, store.size());
    }

    @Test
    void testContains() {
        Bytes key = Bytes.wrap("key");
        Bytes value = Bytes.wrap("value");

        store.put(key, value);
        assertTrue(store.contains(key));
    }

    @Test
    void testSize() {
        assertEquals(0, store.size());

        Bytes key = Bytes.wrap("key");
        Bytes value = Bytes.wrap("value");

        store.put(key, value);
        assertEquals(1, store.size());
    }
}