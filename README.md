<p align="center">
   <img src="./docs/logo.png" alt="KiWi Logo" width="200" height="220"/>
</p>

# KiWi: A Lightweight RESP Key-Value Store.

KiWi is a high-performance, [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/)
compliant, key-value store inspired by the Bitcask paper, designed
for simplicity, reliability, and blazing-fast read/write operations.

## Features

- RESP protocol support for Redis-compatible client interaction.
- Non-blocking I/O server with Netty.
- High-performance key-value store based on the Bitcask storage model.
- In-memory indexing for fast reads.
- TTL-based key expiration.
- Checksums for data integrity.
- Compaction and efficient file merging process.
- Hint files for quick startup times.
- Tunable durability.

## Quick Start

1. Start Docker container
   ```bash
   docker run --rm --name kiwi -p 6379:6379 nemanjam/kiwi:latest
   ```

2. Connect to the server with `redis-cli`
   ```bash
   redis-cli -h localhost
   ```

3. Use the server as you would a Redis server.
   ```text
   SET key value
   OK

   GET key
   "value"

   EXISTS key
   (integer) 1

   DEL key
   OK

   EXISTS key
   (integer) 0
   ```

### Supported Commands

- `SET key value`
- `GET key`
- `DEL key`
- `EXISTS key`
- `FLUSHDB`
- `PING`
- `DBSIZE`
- `INFO`

## Installation

### Prerequisites

- Java 21
- Docker (optional, for running via a container)

### From Source

1. Clone the repository:
   ```bash
   git clone https://github.com/nemanjam/kiwi.git
   cd kiwi
   ```
2. Build & install the project:
   ```bash
   ./gradlew installDist
   ```
3. Run the KiWi server:
   ```bash
   ./kiwi-server/build/install/kiwi-server/bin/kiwi-server
   ```

### Docker

1. Build the Docker image
   ```bash
   docker build -t kiwi .
   ```
2. Run the container:
   ```bash
    docker run --rm --name kiwi -p 6379:6379 kiwi:latest
   ```
3. Connect to the server:
    ```bash
    redis-cli -h localhost -p 6379
    ```

## Configuration

KiWi can be configured using environment variables or a HOCON configuration file.
Refer to [Typesafe Config](https://github.com/lightbend/config) for configuration examples.

Default values are:

- Storage [application.conf](kiwi-core/src/main/resources/application.conf)
- Server [application.conf](kiwi-server/src/main/resources/application.conf)

## Benchmarks

KiWi can be evaluated with [redis-benchmark](https://redis.io/topics/benchmarks) utility command.

Below are the results of running `redis-benchmark` with KiWi and Redis on a local setup (MacBook M3
Pro with 18GB RAM and Sequoia 15.1.1).

```text
redis-benchmark -h localhost -t set -n 100000 -r 10000000 -d 1024

====== SET ======
  100000 requests completed in 2.18 seconds
  50 parallel clients
  1024 bytes payload
  keep alive: 1
  host configuration "save":
  host configuration "appendonly":
  multi-thread: no

Summary:
 throughput summary: 45934.77 requests per second
 latency summary (msec):
          avg       min       p50       p95       p99       max
        1.016     0.088     0.951     1.863     2.703    30.655
```

```text
redis-benchmark -h localhost -t get -n 100000 -r 10000000 -d 1024

====== GET ======
  100000 requests completed in 1.83 seconds
  50 parallel clients
  1024 bytes payload
  keep alive: 1
  host configuration "save":
  host configuration "appendonly":
  multi-thread: no

Summary:
 throughput summary: 60753.34 requests per second
 latency summary (msec):
          avg       min       p50       p95       p99       max
        0.661     0.088     0.639     0.967     1.311    12.255
```

JVM options:

```text
-Xms2g -Xmx2g -XX:UseG1GC –XX:+UseStringDeduplication -XX:+AlwaysPreTouch
````

KiWi Configuration:

```hocon
kiwi {
  storage {
    log {
      dir = "/tmp/kiwi"
      segment.bytes = 1073741824 // 1GB

      sync {
        mode = "periodic"
        periodic {
          interval = 10s
        }
      }
    }
  }
}
```

## Design

KiWi combines the simplicity of RESP with the efficient storage model described in the Bitcask
paper. This architecture is designed for high performance and simplicity.

### Storage Model

- All write operations are appended to a log file, ensuring sequential disk writes for maximum
  performance.
- When the active log file reaches a configurable size, it is rolled over to a segment file.
- Periodically, segment files are compacted to remove stale data and reclaim disk space.
- Crash recovery is achieved by replaying the log files during startup.
- Disk I/O operations, like log compaction, are handled in background threads to avoid blocking
  client requests.

### In-Memory Index

- All keys are stored in an in-memory hash table, pointing to their location in the log file.
- This ensures `O(1)` read performance while keeping the storage footprint minimal.

### Non-Blocking I/O Server

- Netty-based event loop for handling client requests.
- KiWi supports the RESP protocol, making it compatible with Redis clients and tools.

### Durability

- KiWi provides tunable durability options to balance performance and data safety:
    - `periodic` (default): Writes are flushed to disk at regular intervals.
    - `batch`: Writes are batched and flushed when the batch window expires. All writers are blocked
      until the batch is written.
    - `lazy`: Flush is delegated to the operating system, which may delay writes for performance.

### Pros

- Fast writes due to sequential disk I/O.
- Fast reads with O(1) lookups using the in-memory index.
- Simple and robust crash recovery with the data and hint files.
- Incremental crash-safe compaction process.

### Cons

- The in-memory index requires all keys to fit in memory.
- Log compaction introduces periodic I/O overhead.

## Contributing

We welcome contributions to KiWi! Here’s how you can help:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a clear description of your changes.

## License

KiWi is licensed under the MIT License. See [LICENSE](./LICENSE) for details.
