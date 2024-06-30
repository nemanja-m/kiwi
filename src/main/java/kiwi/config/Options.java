package kiwi.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Path;

public class Options {
    private static final Config config = ConfigFactory.load();

    public static final Options DEFAULTS = new Options();

    public Options() {
        this.storageOptions = new Storage(config.getConfig("kiwi.storage"));
    }

    public static class Storage {
        private final Path logDir;
        private final long segmentSize;

        public Storage(Config config) {
            this.logDir = Path.of(config.getString("log.dir"));
            this.segmentSize = config.getLong("log.segment.bytes");
        }

        public Path getLogDir() {
            return logDir;
        }

        public long getSegmentSize() {
            return segmentSize;
        }
    }

    private final Storage storageOptions;

    public Storage getStorageOptions() {
        return storageOptions;
    }
}
