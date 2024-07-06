package kiwi.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Path;
import java.time.Duration;

public class Options {
    private static final Config config = ConfigFactory.load();

    public static final Options defaults = new Options();

    public final Storage storage;

    public Options() {
        this.storage = new Storage(config.getConfig("kiwi.storage"));
    }

    public static class Storage {
        public final Log log;

        public Storage(Config config) {
            this.log = new Log(config.getConfig("log"));
        }

        public static class Log {
            public final Path dir;
            public final long segmentBytes;
            public final Compaction compaction;

            public Log(Config config) {
                this.dir = Path.of(config.getString("dir"));
                this.segmentBytes = config.getLong("segment.bytes");
                this.compaction = new Compaction(config.getConfig("compaction"));
            }

            public static class Compaction {
                public final Duration interval;
                public final double minFragmentationRatio;

                public Compaction(Config config) {
                    this.interval = config.getDuration("interval");
                    this.minFragmentationRatio = config.getDouble("min-fragmentation-ratio");
                }
            }
        }
    }

}
