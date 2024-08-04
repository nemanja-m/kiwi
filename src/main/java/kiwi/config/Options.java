package kiwi.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kiwi.storage.config.StorageConfig;

public class Options {

    private static final Config config = ConfigFactory.load();
    public static final Options defaults = new Options();

    public final StorageConfig storage;

    public Options() {
        this.storage = new StorageConfig(config.getConfig("kiwi.storage"));
    }

}
