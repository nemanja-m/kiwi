package kiwi.embedded;

import kiwi.core.common.Bytes;
import kiwi.core.config.Options;
import kiwi.core.storage.KeyValueStore;
import kiwi.core.storage.bitcask.BitcaskStore;
import kiwi.core.storage.config.StorageConfig;

import java.nio.file.Path;
import java.util.Optional;

public class Kiwi implements KeyValueStore<Bytes, Bytes> {
    private final KeyValueStore<Bytes, Bytes> store;

    Kiwi(KeyValueStore<Bytes, Bytes> store) {
        this.store = store;
    }

    public static Kiwi open() {
        return open(Options.defaults.storage);
    }

    public static Kiwi open(Path logDir) {
        return new Kiwi(BitcaskStore.open(logDir));
    }

    public static Kiwi open(StorageConfig storageConfig) {
        return new Kiwi(BitcaskStore.open(storageConfig));
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

    @Override
    public void close() throws Exception {
        store.close();
    }
}
