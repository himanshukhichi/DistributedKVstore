# Distributed KV store

DistKV is a Java 17 distributed key-value store project built for demonstrating the internals behind Dynamo-style storage systems: gRPC APIs, consistent hashing, quorum replication, WAL-backed storage, and gossip membership.

## Stack

- Java 17
- gRPC + Protocol Buffers
- Netty via `grpc-netty-shaded`
- Maven
- JUnit 5
- Mockito

## Core Features Implemented

- `KVService` protobuf API: `Get`, `Put`, `Delete`, and server-streaming `Scan`.
- Per-request `ConsistencyLevel`: `ONE`, `QUORUM`, `ALL`.
- `AdminService` protobuf API: `ClusterStatus`, `NodeJoin`, `NodeLeave`.
- MD5-based consistent hash ring with configurable virtual nodes, defaulting to 150.
- Preference list lookup returning distinct physical replicas in clockwise ring order.
- Quorum coordinator for parallel replica writes and reads.
- Internal gRPC replica service/client for node-to-node read/write fanout.
- In-memory thread-safe storage using `ConcurrentHashMap`.
- Write-ahead log that appends every write/delete before memory is updated and replays on restart.
- Gossip membership model with heartbeat counters, fanout of 2 peers per cycle, and suspect marking after 3 missed cycles.
- Bloom filter implementation with configurable false-positive rate.
- Unit tests for hash-ring distribution, WAL replay, Bloom filter false-positive rate, and quorum combinations.

## Project Layout

```text
src/main/proto/kv.proto          gRPC service and message definitions
src/main/java/com/distkv/routing Consistent hash ring
src/main/java/com/distkv/storage Per-node storage, WAL, Bloom filter
src/main/java/com/distkv/replication Quorum coordinator and replica clients
src/main/java/com/distkv/membership Gossip membership types
src/main/java/com/distkv/grpc     gRPC service implementations
src/test/java/com/distkv          Core unit tests
```

## Build And Test

```bash
mvn test
```

Run a single local node:

```bash
mvn exec:java
```

Useful environment variables:

```bash
DISTKV_NODE_ID=node-1
DISTKV_HOST=localhost
DISTKV_PORT=50051
DISTKV_REPLICATION_FACTOR=3
DISTKV_DATA_DIR=data/node-1
DISTKV_PEERS=node-2:localhost:50052,node-3:localhost:50053
```

## Bloom Filter Math

For expected insertions `n` and false-positive probability `p`:

```text
m = ceil(-(n * ln(p)) / (ln(2)^2))
k = round((m / n) * ln(2))
```

With `n = 1000` and `p = 0.01`, DistKV uses `m = 9586` bits and `k = 7` hash functions.

## Notes
