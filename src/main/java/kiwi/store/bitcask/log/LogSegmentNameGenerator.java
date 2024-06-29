package kiwi.store.bitcask.log;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LogSegmentNameGenerator {
    private long counter;

    public LogSegmentNameGenerator() {
        this(0);
    }

    public LogSegmentNameGenerator(long counter) {
        this.counter = counter;
    }

    public static LogSegmentNameGenerator from(LogSegment segment) {
        long counter = Long.parseLong(segment.name()) + 1;
        return new LogSegmentNameGenerator(counter);
    }

    public Path next() {
        return next(Paths.get("."));
    }

    public Path next(Path directory) {
        // Max number of segments is 64-bit unsigned long.
        return directory.resolve(String.format("%020d.log", counter++));
    }
}
