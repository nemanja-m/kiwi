package kiwi.storage;

import kiwi.common.Bytes;
import kiwi.config.Options;
import kiwi.storage.bitcask.BitcaskStore;

import java.nio.file.Path;
import java.util.Optional;

public class KiwiStore implements KeyValueStore<Bytes, Bytes> {
    private final KeyValueStore<Bytes, Bytes> store;

    KiwiStore(KeyValueStore<Bytes, Bytes> store) {
        this.store = store;
    }

    public static KiwiStore open() {
        return open(Options.DEFAULTS.getStorageOptions());
    }

    public static KiwiStore open(Path root) {
        return new KiwiStore(BitcaskStore.open(root));
    }

    public static KiwiStore open(Options.Storage options) {
        return new KiwiStore(BitcaskStore.open(options));
    }

    @Override
    public void put(Bytes key, Bytes value) {
        store.put(key, value);
    }

    @Override
    public void put(Bytes key, Bytes value, long ttl) {
        store.put(key, value, ttl);
    }

    @Override
    public Optional<Bytes> get(Bytes key) {
        return store.get(key);
    }

    @Override
    public void delete(Bytes key) {
        store.delete(key);
    }

    @Override
    public boolean contains(Bytes key) {
        return store.contains(key);
    }

    @Override
    public int size() {
        return store.size();
    }
}
