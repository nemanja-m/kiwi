package kiwi.storage.bitcask.log.sync;

import kiwi.common.NamedThreadFactory;
import kiwi.storage.bitcask.log.LogSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PeriodicSegmentWriter extends SegmentWriter {
    private static final Logger logger = LoggerFactory.getLogger(PeriodicSegmentWriter.class);

    private final Duration syncInterval;
    private final ScheduledExecutorService scheduler;

    public PeriodicSegmentWriter(Supplier<LogSegment> activeSegmentSupplier, Duration syncInterval) {
        super(activeSegmentSupplier);

        this.syncInterval = syncInterval;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory.create("periodic-sync"));
        this.scheduler.scheduleAtFixedRate(() -> {
                    if (!closed.get()) {
                        sync();
                        logger.info("Synced active segment");
                    }
                },
                syncInterval.toMillis(),
                syncInterval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void close() {
        super.close();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(15, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            logger.error("Error while shutting down periodic sync scheduler", e);
        }
    }
}
