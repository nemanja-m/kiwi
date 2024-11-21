package kiwi.storage.bitcask.log.sync;

import kiwi.error.KiwiWriteException;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.Record;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class SegmentWriter implements AutoCloseable {
    protected final Supplier<LogSegment> activeSegmentSupplier;
    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public SegmentWriter(Supplier<LogSegment> activeSegmentSupplier) {
        this.activeSegmentSupplier = activeSegmentSupplier;
    }

    /**
     * Append a record to the active segment.
     *
     * <p>Implementations should ensure that the record is written to the underlying storage</p>
     *
     * @param record the record to append
     * @throws KiwiWriteException if an error occurs while writing the record
     */
    public int append(Record record) throws KiwiWriteException {
        if (closed.get()) {
            throw new KiwiWriteException("Segment writer is closed");
        }

        return activeSegment().append(record);
    }

    protected void sync() {
        if (!closed.get()) {
            activeSegment().sync();
        }
    }

    protected LogSegment activeSegment() {
        return activeSegmentSupplier.get();
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            activeSegment().close();
        }
    }
}
