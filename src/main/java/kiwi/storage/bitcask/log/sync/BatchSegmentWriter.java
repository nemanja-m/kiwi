package kiwi.storage.bitcask.log.sync;

import kiwi.storage.bitcask.log.LogSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

// TODO: Implement this class.
public class BatchSegmentWriter extends SegmentWriter {
    private static final Logger logger = LoggerFactory.getLogger(BatchSegmentWriter.class);

    private final Duration window;

    public BatchSegmentWriter(Supplier<LogSegment> activeSegmentSupplier, Duration window) {
        super(activeSegmentSupplier);

        this.window = window;
    }
}
