package kiwi.storage.bitcask.sync;

import kiwi.error.KiwiWriteException;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.Record;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class SegmentWriter implements AutoCloseable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Supplier<LogSegment> activeSegmentSupplier;

    public SegmentWriter(Supplier<LogSegment> activeSegmentSupplier) {
        this.activeSegmentSupplier = activeSegmentSupplier;
    }

    abstract protected void append(Record record) throws KiwiWriteException;

    protected void sync() {
        activeSegment().sync();
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
