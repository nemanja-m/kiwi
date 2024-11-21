package kiwi.storage.bitcask.sync;

import kiwi.error.KiwiWriteException;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.Record;

import java.util.function.Supplier;

/**
 * A {@link SegmentWriter} that appends records to the active segment and defers
 * fsync to operating system. This is useful for performance sensitive applications but
 * may result in data loss in case of a crash.
 */
public class LazySegmentWriter extends SegmentWriter {
    public LazySegmentWriter(Supplier<LogSegment> activeSegmentSupplier) {
        super(activeSegmentSupplier);
    }

    @Override
    protected void append(Record record) throws KiwiWriteException {
        activeSegment().append(record);
    }
}
