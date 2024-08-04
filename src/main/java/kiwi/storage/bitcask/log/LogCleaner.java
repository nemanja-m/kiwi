package kiwi.storage.bitcask.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogCleaner {
    private static final Logger logger = LoggerFactory.getLogger(LogCleaner.class);

    private final Duration interval;
    private final ScheduledExecutorService scheduler;

    public LogCleaner(Duration interval) {
        this.interval = interval;
        this.scheduler = Executors.newScheduledThreadPool(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down log cleaner ...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.MINUTES)) {
                    logger.warn("Log cleaner did not shutdown in time. Forcing shutdown.");
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException ex) {
                logger.error("Failed to shutdown log cleaner", ex);
                scheduler.shutdownNow();
            }
        }));
    }

    public void schedule(Runnable task) {
        scheduler.scheduleAtFixedRate(task, interval.toMinutes(), interval.toMinutes(), TimeUnit.MINUTES);
    }
}
