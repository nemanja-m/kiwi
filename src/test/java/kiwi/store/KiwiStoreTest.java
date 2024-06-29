package kiwi.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

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
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        store.put(key, value);

        Optional<byte[]> retrievedValue = store.get(key);
        assertArrayEquals(value, retrievedValue.orElseThrow());
        assertEquals(1, store.size());
    }

    @Test
    void testGetNonExistentKey() {
        byte[] key = "key".getBytes();
        Optional<byte[]> retrievedValue = store.get(key);
        assertTrue(retrievedValue.isEmpty());
    }

    @Test
    void testDelete() {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        store.put(key, value);
        store.delete(key);

        Optional<byte[]> retrievedValue = store.get(key);
        assertTrue(retrievedValue.isEmpty());
        assertEquals(0, store.size());
    }

    @Test
    void testContains() {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        store.put(key, value);
        assertTrue(store.contains(key));
    }

    @Test
    void testSize() {
        assertEquals(0, store.size());

        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();

        store.put(key, value);
        assertEquals(1, store.size());
    }
}