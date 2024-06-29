package kiwi.store;

import kiwi.store.bitcask.BitcaskStore;

import java.nio.file.Path;
import java.util.Optional;

public class KiwiStore implements KeyValueStore<byte[], byte[]> {
    private final KeyValueStore<byte[], byte[]> store;

    KiwiStore(KeyValueStore<byte[], byte[]> store) {
        this.store = store;
    }

    public static KiwiStore open(Path root) {
        return new KiwiStore(BitcaskStore.open(root));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        store.put(key, value);
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return store.get(key);
    }

    @Override
    public void delete(byte[] key) {
        store.delete(key);
    }

    @Override
    public boolean contains(byte[] key) {
        return store.contains(key);
    }

    @Override
    public int size() {
        return store.size();
    }
}
