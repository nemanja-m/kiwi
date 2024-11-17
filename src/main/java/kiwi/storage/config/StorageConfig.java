package kiwi.storage.config;

import com.typesafe.config.Config;
import kiwi.storage.bitcask.log.config.LogConfig;

public class StorageConfig {
    public final LogConfig log;

    public StorageConfig(Config config) {
        this.log = new LogConfig(config.getConfig("log"));
    }
}
