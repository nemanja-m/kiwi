package kiwi.storage.bitcask.log.config;

import com.typesafe.config.Config;

import java.nio.file.Path;
import java.time.Duration;

public class LogConfig {
    public final Path dir;
    public final long segmentBytes;
    public final Compaction compaction;

    public LogConfig(Config config) {
        this.dir = Path.of(config.getString("dir"));
        this.segmentBytes = config.getLong("segment.bytes");
        this.compaction = new Compaction(config.getConfig("compaction"));
    }

    public static class Compaction {
        public final Duration interval;
        public final double minDirtyRatio;
        public final long segmentMinBytes;

        public Compaction(Config config) {
            this.interval = config.getDuration("interval");
            this.minDirtyRatio = config.getDouble("min.dirty.ratio");
            this.segmentMinBytes = config.getLong("segment.min.bytes");
        }
    }
}
