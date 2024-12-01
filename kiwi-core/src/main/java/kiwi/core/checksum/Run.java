package kiwi.core.checksum;

import kiwi.core.storage.bitcask.log.LogSegment;
import kiwi.core.storage.bitcask.log.Record;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class Run {

    public static void main(String[] args) throws IOException {
        Path logDir = null;
        int numThreads = Runtime.getRuntime().availableProcessors();

        if (args.length == 0) {
            System.err.println("Usage: COMMAND --dir <log-dir> [--threads <num-threads>]");
            System.exit(1);
        }

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dir":
                case "-d":
                    if (i + 1 < args.length) {
                        try {
                            logDir = Paths.get(args[++i]);

                            if (!logDir.toFile().isDirectory()) {
                                System.err.println("Path for --dir or -d is not a directory");
                                System.exit(1);
                            }
                        } catch (InvalidPathException e) {
                            System.err.println("Invalid path for --dir or -d");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Missing value for --dir or -d");
                        System.exit(1);
                    }
                    break;
                case "--threads":
                case "-t":
                    if (i + 1 < args.length) {
                        try {
                            numThreads = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid number format for --threads or -t");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Missing value for --threads or -t");
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }

        if (logDir == null) {
            System.err.println("Argument '--dir' is required");
            System.exit(1);
        }

        checksum(logDir, numThreads);

        System.exit(0);
    }

    static void checksum(Path logDir, int numThreads) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(logDir)) {
            List<LogSegment> segments = paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".log"))
                    .map((path) -> LogSegment.open(path, true))
                    .toList();

            for (LogSegment segment : segments) {
                futures.add(executor.submit(() -> checkLogSegment(segment)));
            }
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (Exception e) {
                System.err.println("Error processing log: " + e.getMessage());
            }
        });

        executor.shutdown();
    }

    static void checkLogSegment(LogSegment segment) {
        long position = 0;
        for (Record record : segment.getRecords()) {
            if (!record.isValidChecksum()) {
                String message = String.format(
                        "Checksum failed: segment=%s position=%s checksum=%d timestamp=%d ttl=%d keySize=%d valueSize=%d",
                        segment.name(),
                        position,
                        record.header().checksum(),
                        record.header().timestamp(),
                        record.header().ttl(),
                        record.header().keySize(),
                        record.header().valueSize());
                System.out.println(message);
            }
            position += record.size();
        }
    }
}
