# DistKV Learning Guide: Complete Architecture & Implementation

This guide takes you from zero to understanding the entire Distributed KV Store, including every component interaction, data flow, key concepts, and how to test everything.

---

## Table of Contents

1. [High-Level Architecture](#high-level-architecture)
2. [Core Concepts](#core-concepts)
3. [Layer-by-Layer Breakdown](#layer-by-layer-breakdown)
4. [Data Flow Examples](#data-flow-examples)
5. [Testing Strategy](#testing-strategy)
6. [Running Demos](#running-demos)
7. [Interview Talking Points](#interview-talking-points)

---

## High-Level Architecture

### System Overview

DistKV is a **Dynamo-style distributed key-value store**. Imagine it as:
- **N nodes** in a cluster, each holding a copy of some keys
- **Every write** goes to **W replicas** (write quorum)
- **Every read** comes from **R replicas** (read quorum)
- **R + W > N** guarantees reads always see the most recent writes

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ Put("key", "value", QUORUM)
       ▼
┌──────────────────────────────┐
│  Node 1 (Coordinator)        │
│  - Hash the key              │
│  - Get preference list [N1,N2,N3] │
└──────────────────────────────┘
       │
   ┌───┼───┐
   ▼   ▼   ▼
 ┌──┐ ┌──┐ ┌──┐
 │N1│ │N2│ │N3│  ← Fan out in parallel
 └──┘ └──┘ └──┘
   │   │   │
   └───┼───┘
       │ (wait for W=2 acks)
       ▼
     SUCCESS
```

### The 5 Key Layers

| Layer | Purpose | Components |
|-------|---------|------------|
| **gRPC API** | Client interface | KVService, AdminService, ReplicaService |
| **Routing** | Map keys to nodes | Consistent hash ring, preference list |
| **Replication** | Coordinate across nodes | QuorumCoordinator, replica clients |
| **Storage** | Persist data locally | WAL, in-memory store, Bloom filter |
| **Membership** | Track node health | Gossip protocol, failure detection |

---

## Core Concepts

### 1. Consistent Hashing

**Problem**: When you add/remove a node, you don't want to rehash all keys.

**Solution**: Use a hash ring.

```
Ring positions: 0 to 2^128

    Node A
      |
   ---+---
  /       \
 /         \  Node B
|           |
 \    N    /
  \       /
   ---+---
      |
    Node C
```

**How it works**:
- Each node gets `virtualNodes` (default 150) tokens on the ring
- To find replicas for a key: hash the key → find that position → walk clockwise → collect N distinct *physical* nodes

**Why 150 virtual nodes?**
- Better load balancing (uniform distribution)
- When a node fails, fewer keys move to remaining nodes
- Trade-off: more memory per node to track tokens

**Implementation**: `ConsistentHashRing.java:64-80`
```java
List<NodeEndpoint> replicas = ring.getPreferenceList(key, replicationFactor)
// Returns [Node1, Node2, Node3] in clockwise order
```

### 2. Quorum Consistency

**Problem**: How many replicas must acknowledge before declaring success?

**Solution**: Configure read/write quorums.

```
N = 3 (replication factor)
W = 2 (write quorum: must get 2 acks)
R = 2 (read quorum: must read from 2 replicas)

R + W > N  →  2 + 2 > 3  ✓ (guarantees overlap)
```

**Three consistency levels**:

| Level | Write acks needed | Read replicas contacted | Use case |
|-------|-------------------|------------------------|----------|
| **ONE** | 1 (fastest) | 1 (fastest) | Cache, non-critical |
| **QUORUM** | ceil(N/2)+1 = 2 | 2 | Default, good balance |
| **ALL** | All 3 (slowest) | All 3 (slowest) | Critical data |

**Why R + W > N matters**:
```
Scenario: N=3, W=2, R=2

Write to [N1, N2]:
  N1: key="foo"
  N2: key="foo"
  N3: (stale)

Read from [N1, N2]:
  Will see "foo" on at least one replica
  Because R + W > N, we must overlap
```

**Implementation**: `QuorumCoordinator.java:62-96`

### 3. Vector Clocks

**Problem**: If two writes happen on different nodes at roughly the same time, which one is newer?

**Solution**: Track causality with vector clocks.

**Vector Clock**: A map `{nodeId → counter}`

Example sequence:
```
Node A writes:  v1 = {A:1}
Node B writes:  v2 = {B:1}
Node A writes again: v3 = {A:2, B:1}  ← A learned about B's write

v3 > v1 because {A:2, B:1} dominates {A:1}
v3 vs v2: CONCURRENT (not ordered)
```

**Comparison logic** (`VersionedValue.compareVectorClock`):
- If all counters in v1 ≥ v2 AND at least one > : v1 is AFTER
- If all counters in v1 ≤ v2 AND at least one < : v1 is BEFORE
- Otherwise: CONCURRENT (conflict!)

**Conflict handling**:
- Store both versions as "siblings"
- Return latest visible value by timestamp to client
- Application can reconcile (last-write-wins, merge, etc.)

**Implementation**: `VersionedValue.java:47-83`

### 4. Write-Ahead Logging (WAL)

**Problem**: If a node crashes, you lose all in-memory data.

**Solution**: Write to disk BEFORE updating memory.

**WAL Protocol**:
```
1. Append(key="x", value="100") to disk file
   (OS buffers it, will sync to disk)

2. Update in-memory store
   store.put("x", "100")

3. On crash:
   - In-memory store is lost
   - Read WAL file from disk
   - Replay: put("x", "100")
   - Recover state!
```

**Performance optimization**: WAL compaction
```
After 10,000 writes:
- Write a full snapshot to "wal.snapshot"
- Truncate the WAL log
- On restart: Load snapshot (fast) + replay remaining WAL

Without compaction: WAL grows unbounded
```

**Implementation**: `WALManager.java:54-76`

### 5. Bloom Filter

**Problem**: Every Get checks memory. If key doesn't exist, still costs a lookup.

**Solution**: Use a Bloom filter for fast "definitely not here" checks.

**Bloom Filter Basics**:
- Array of `m` bits, `k` hash functions
- To insert a key: hash it `k` times, set those bits
- To check: hash it `k` times, check if all bits are set
  - All bits set → *might be present* (need to check storage)
  - At least one bit unset → *definitely not present* (skip storage)

**False positive rate math**:
```
m = ceil(-(n * ln(p)) / (ln(2)^2))
k = round((m / n) * ln(2))

For n=100k keys, p=0.01 (1% false positive):
m ≈ 958,506 bits ≈ 120 KB
k ≈ 7 hash functions
```

**When to use**: Good for read-heavy workloads. Not good if you delete keys often (can't really "delete" from a Bloom filter).

**Implementation**: `BloomFilter.java`

### 6. Hinted Handoff

**Problem**: Node N2 goes down temporarily. Writes meant for N2 fail.

**Solution**: If N2 is down, store the write on the coordinator node as a "hint". When N2 comes back, deliver the hints.

```
Client write: Put("key", "value", QUORUM)
Preference list: [N1, N2, N3]

N2 is DOWN:
  N1 writes locally + stores hint for N2
  N3 writes
  (2 acks = QUORUM reached, return success)

N2 comes back alive:
  Gossip detects N2 is alive
  N1 sees hints pending for N2
  N1 delivers hints to N2
  (N2 now has the value)

This is "sloppy quorum" - quorum is satisfied even if
one replica was missing, because hint ensures eventual delivery
```

**Implementation**: `HintedHandoffManager.java`

### 7. Anti-Entropy Repair

**Problem**: If a node was down for a long time, hints alone might not recover all missing writes.

**Solution**: Periodically compare replicas using Merkle trees and sync diverged keys.

```
Node A Merkle tree:
  SHA256(key1_version)
  SHA256(key2_version)
  ...
  └─ Root: ABC123

Node B Merkle tree:
  SHA256(key1_version)
  SHA256(key2_version)
  ...
  └─ Root: XYZ789

Roots differ → trees diverged
  Walk both trees, find differing leaves
  For each differing key, fetch versions from A
  Merge into B
```

**Implementation**: `MerkleTree.java`, `AntiEntropyService.java`

### 8. Gossip Protocol (Membership)

**Problem**: How do nodes know who is alive without a central coordinator?

**Solution**: Gossip randomly with peers, exchange membership lists.

```
Every second (default):

1. Node increments its own heartbeat counter
   local_heartbeat = 5

2. Pick 2 random peers, send:
   [
     {nodeId: "node-1", heartbeat: 5, status: ALIVE},
     {nodeId: "node-2", heartbeat: 3, status: SUSPECT},
     ...
   ]

3. Receive their lists, merge:
   - If remote is newer → accept it
   - If local is newer → ignore remote

4. Mark nodes SUSPECT if counter hasn't increased in 3 cycles
   Mark DEAD after 6 cycles
   Remove from hash ring when DEAD
```

**Why it works**:
- Information spreads exponentially (O(log n) rounds)
- No single point of failure
- Tolerates network partitions (temporary inconsistency, not data loss)

**Implementation**: `GossipService.java:63-78`, `MembershipList.java`

---

## Layer-by-Layer Breakdown

### Layer 1: gRPC API

**Files**: `KVServiceImpl.java`, `AdminServiceImpl.java`, `ReplicaServiceImpl.java`, `kv.proto`

**What it does**: Handle client RPC calls, fan them out to the routing/replication layer.

**Main methods**:

```java
// Client-facing RPC
KVService.Get(GetRequest) → GetResponse
KVService.Put(PutRequest) → PutResponse
KVService.Delete(DeleteRequest) → DeleteResponse
KVService.Scan(ScanRequest with ConsistencyLevel) → stream Entry

AdminService.ClusterStatus() → ClusterStatusResponse
AdminService.NodeJoin(node) → NodeJoinResponse
AdminService.NodeLeave(nodeId) → NodeLeaveResponse

// Node-to-node RPC (internal)
ReplicaService.Apply(key, version) → success
ReplicaService.Read(key) → versions
ReplicaService.Merkle(startKey, endKey) → root_hash + leaf_hashes
```

**Key point**: The gRPC layer **translates** between Protocol Buffers (wire format) and Java objects (`VersionedValue`, `NodeEndpoint`, etc.). All KV requests carry `ConsistencyLevel`; Scan keeps the field for API symmetry but streams a local range from the contacted node.

The Java client exposes both `scanStreaming(...)`, which returns an `Iterator<Entry>` over the server stream, and `scan(...)`, which collects that stream into a list for simple demos.

**Example flow** (`KVServiceImpl.put()`):
```
1. Receive PutRequest(key, value, consistency)
2. If this node is not first in the key's preference list, forward to that coordinator
3. Call coordinator.put(key, value, consistency)
4. Get WriteQuorumResult
5. Convert to PutResponse
6. Return to client
```

**Data model** (in `.proto`):
```protobuf
message ValueVersion {
  bytes value = 1;
  int64 timestamp_epoch_ms = 2;
  map<string, int64> vector_clock = 3;
  bool tombstone = 4;
}

ConsistencyLevel {
  ONE = 0;
  QUORUM = 1;
  ALL = 2;
}
```

### Layer 2: Routing (Consistent Hash Ring)

**Files**: `ConsistentHashRing.java`, `NodeEndpoint.java`

**What it does**: Map a key to a list of replica nodes.

**Key methods**:

```java
ring.addNode(NodeEndpoint)           // Add node with 150 virtual tokens
ring.removeNode(nodeId)              // Remove when node dies
ring.getPreferenceList(key, 3)       // [N1, N2, N3] in ring order
ring.getCoordinator(key)             // N1 (first replica)
```

The first node in the preference list is the coordinator for Get/Put/Delete. The Java client still picks a random healthy node, so a non-coordinator node forwards the request to the correct coordinator before the quorum fan-out.

**Ring structure**:
```
NavigableMap<BigInteger, NodeEndpoint> ring
├─ Token: hash("node-1#0")   → node-1
├─ Token: hash("node-1#1")   → node-1
├─ Token: hash("node-1#2")   → node-1
│  ...
├─ Token: hash("node-2#0")   → node-2
│  ...
└─ Token: hash("node-3#0")   → node-3
```

**Lookup algorithm** (O(log n)):
```
To find replicas for "mykey":
  1. token = hash("mykey")
  2. ring.tailMap(token, true)  → tokens ≥ token
     Collect first distinct physical nodes
  3. If still need more, ring.headMap(token, false) → wrap around
```

**Why MD5 hashing?**
- Distributes keys uniformly
- Deterministic (same key always hashes to same position)
- Fast enough (thousands of hashes per second)

### Layer 3: Replication (Quorum Coordinator)

**Files**: `QuorumCoordinator.java`, `QuorumCalculator.java`, `ReplicaClient.java` (interface), `GrpcReplicaClient.java`, `LocalReplicaClient.java`

**What it does**: Orchestrate reads/writes across replicas, wait for quorum acks.

**Key methods**:

```java
coordinator.put(key, value, QUORUM)    // Write with quorum
coordinator.get(key, QUORUM)           // Read with quorum
coordinator.delete(key, QUORUM)        // Delete (tombstone) with quorum
```

**Put flow**:

```java
public WriteQuorumResult put(String key, byte[] value, ConsistencyLevel level) {
  1. Create VersionedValue:
     version = VersionedValue.put(value, "node-1", counter++, clock)
     // {nodeId: "node-1", counter: 42, timestamp: 1234567890}

  2. Get replicas:
     replicas = ring.getPreferenceList(key, replicationFactor=3)
     // [N1, N2, N3]

  3. Send to all in parallel:
     futures = [
       replicaClient.write(N1, key, version),
       replicaClient.write(N2, key, version),
       replicaClient.write(N3, key, version),
     ]

  4. Wait for W acks:
     requiredAcks = QuorumCalculator.requiredAcks(3, QUORUM) = 2
     acks = 0
     for future in futures:
       if future.get(timeout):
         acks++
       if acks >= requiredAcks:
         return SUCCESS

  5. If hinted handoff enabled and a replica was down:
     hintedHandoff.store(unreachableNode, key, version)
}
```

**Get flow**:

```java
public ReadQuorumResult get(String key, ConsistencyLevel level) {
  1. Get replicas: [N1, N2, N3]

  2. Read from all in parallel:
     futures = [
       replicaClient.read(N1, key),
       replicaClient.read(N2, key),
       replicaClient.read(N3, key),
     ]

  3. Collect responses:
     versions = []
     responses = 0
     for future in futures:
       if future.get(timeout):
         responses++
         versions.addAll(future.result.versions)

  4. Check if quorum reached:
     requiredResponses = QuorumCalculator.requiredResponses(3, QUORUM) = 2
     if responses < 2:
       return ERROR

  5. Return newest non-tombstone version:
     latest = VersionedValue.latest(versions)
     return latest
}
```

**Quorum Math** (`QuorumCalculator.java`):

```java
N = 3

ONE:    W = 1, R = 1   (any single node)
QUORUM: W = 2, R = 2   (majority)
ALL:    W = 3, R = 3   (all nodes)

ONE + ONE = 2, NOT > 3  → Can read stale data ✓ (expected)
QUORUM + QUORUM = 4 > 3 → Always overlap ✓
ALL + ALL = 6 > 3      → Always overlap ✓
```

### Layer 4: Storage (WAL + In-Memory Store)

**Files**: `WALManager.java`, `InMemoryKeyValueStore.java`, `BloomFilter.java`, `StoredEntry.java`

**What it does**: Persist and retrieve key-value pairs durably.

The in-memory store is synchronized around mutations and backed by a `ConcurrentHashMap`; the gRPC server uses Java 21 virtual threads for request handlers, so blocking quorum fan-out and WAL work do not consume a fixed platform-thread pool.

**Key methods**:

```java
store.put(key, value)              // Add to memory + WAL
store.get(key)                     // Read from memory
store.delete(key)                  // Mark as deleted in memory + WAL
store.scan(startKey, endKey)       // Range scan
```

**Put operation** (write-ahead log):

```java
public void put(String key, byte[] value) throws IOException {
  1. Append to WAL:
     walManager.append(key, value)
     // Writes to disk (or kernel buffer)

  2. Update memory:
     concurrentStore.put(key, value)
}
```

**Get operation** (with Bloom filter):

```java
public byte[] get(String key) {
  1. Check Bloom filter:
     if (!bloomFilter.contains(key)) {
       return null;  // Definitely not here
     }

  2. Check in-memory store:
     return concurrentStore.get(key)
}
```

**Scan operation** (range query):

```java
public Stream<Entry> scan(String startKey, String endKey) {
  // Returns all keys in [startKey, endKey) sorted
  return concurrentStore.entrySet()
    .stream()
    .filter(e -> e.key >= startKey && e.key < endKey)
    .sorted();
}
```

**LRU capacity controls**:
- `DISTKV_MAX_ENTRIES` caps the number of keys.
- `DISTKV_MAX_MEMORY_BYTES` caps approximate in-memory bytes and evicts least-recently-used keys when the estimate is exceeded.

**Restart recovery**:

```java
public void recover() throws IOException {
  // On server startup
  1. Load snapshot:
     latestSnapshot = walManager.loadSnapshot()
     // Bulk load previous state

  2. Replay WAL delta:
     latestSnapshot.putAll(walManager.replayWalDelta())
     // Apply changes since snapshot

  3. Restore to store:
     concurrentStore.putAll(latestSnapshot)
}
```

**WAL Compaction** (to prevent unbounded growth):

```
After 10,000 writes (configurable):
  1. Write snapshot file (full state)
  2. Truncate WAL
  3. On next restart: load snapshot + smaller WAL delta
```

### Layer 5: Membership & Failure Detection

**Files**: `GossipService.java`, `MembershipList.java`, `MemberInfo.java`, `GrpcGossipPeerClient.java`

**What it does**: Track which nodes are alive, remove dead nodes from the ring.

**Key methods**:

```java
gossip.start()                    // Start gossip loop
gossip.gossipOnce()               // Execute one gossip round
membershipList.addNode(endpoint)  // Add node
membershipList.markFailures()     // Detect and mark dead nodes
```

**Gossip round** (every 1 second):

```java
public void gossipOnce() {
  1. Increment own heartbeat:
     membershipList.incrementLocalHeartbeat()
     // "I'm alive, counter=5"

  2. Pick 2 random peers:
     peers = membershipList.healthyPeers()
     Collections.shuffle(peers)
     peers = peers.stream().limit(fanout=2)

  3. Exchange membership:
     for peer in peers:
       remoteList = peerClient.exchange(peer, localList)
       // Send: [{node-1, hb=5}, {node-2, hb=3}, ...]
       // Receive: [{node-1, hb=4}, {node-3, hb=2}, ...]

       membershipList.merge(remoteList)

  4. Detect failures:
     deadNodes = membershipList.markFailures(
       interval=1s,
       suspectAfterCycles=3,
       deadAfterCycles=6
     )
     // If no increase for 3 cycles: SUSPECT
     // If no increase for 6 cycles: DEAD

  5. Update hash ring:
     for deadNode in deadNodes:
       ring.removeNode(deadNode.nodeId())
       // Rebalance future writes away from dead node
}
```

**State transitions**:

```
ALIVE → (no heartbeat for 3 cycles) → SUSPECT
                                       ↓ (no heartbeat for 3 more cycles)
                                     DEAD (removed from ring)
                                       ↑
                                  (heartbeat received)
                                    ALIVE again
```

---

## Data Flow Examples

### Example 1: Put with QUORUM

**Setup**: 3-node cluster, N=3, W=2, R=2, replication factor=3

**Scenario**: Client calls `put("user:100", "Alice", QUORUM)`

```
Client                Node1 (Coordinator)    Node2          Node3
  │                        │                  │              │
  │ Put request            │                  │              │
  ├──────────────────────>  │                  │              │
  │                        │                  │              │
  │                    Hash key "user:100"    │              │
  │                    Get prefs: [N1,N2,N3]  │              │
  │                        │                  │              │
  │                    Create version:        │              │
  │                    {value: "Alice",       │              │
  │                     nodeId: "N1",         │              │
  │                     counter: 42,          │              │
  │                     ts: 1000}             │              │
  │                        │                  │              │
  │                    Send Apply RPCs in parallel:
  │                        │                  │              │
  │                    Apply to local store   │              │
  │                    Append to WAL          │              │
  │                    Return ack             │              │
  │                        │──────────────────>              │
  │                        │ Apply RPC        │              │
  │                        │                  │ Append to WAL │
  │                        │                  │ Return ack   │
  │                        │                  ├──────────────>
  │                        │                  │              │
  │                        │ (Wait for W=2 acks)            │ Append to WAL
  │                        │ ack1: ✓ from N1  │              │ Return ack
  │                        │ ack2: ✓ from N2  │              │
  │                        │ ack3: ✗ timeout  │              │
  │                        │                  │              │
  │                        │ Acks >= W (2), SUCCESS!         │
  │                        │ Store hint for N3 (down)        │
  │                        │                  │              │
  │ Put response SUCCESS   │                  │              │
  │ <──────────────────────┤                  │              │
  │
  │ (moments later, N3 comes back)
  │
  │                   Gossip detects N3 alive
  │                   Send pending hint to N3
  │                        │                  │              │
  │                        │                  │              │
  │                    Apply hint RPC         │              │
  │                        │──────────────────────────────>  │
  │                        │                  │              │
  │                        │                  │              │
  │                        │                  │   Apply version
  │                        │                  │   (now all 3 have it)
```

**Result**: Key "user:100" is replicated on N1, N2, and N3 with same version.

---

### Example 2: Get with QUORUM (Conflict Scenario)

**Setup**: Same cluster, but data has conflicts.

**State before**:
- N1: `user:100` = {value: "Alice", clock: {N1:10}}, ts: 1000
- N2: `user:100` = {value: "Bob", clock: {N2:5}}, ts: 1100 (concurrent write, never merged)
- N3: `user:100` = {value: "Alice", clock: {N1:10}}, ts: 1000

**Client calls**: `get("user:100", QUORUM)`

```
Client                N1                   N2                N3
  │                   │                    │                 │
  │ Get request       │                    │                 │
  ├──────────────────>│                    │                 │
  │                   │                    │                 │
  │                   Hash key, get prefs: [N1, N2, N3]      │
  │                   Send Read RPCs in parallel:            │
  │                   │ Read RPC           │                 │
  │                   ├──────────────────>│                 │
  │                   │                    │                 │
  │                   │        (also send to N2 and N3)      │
  │                   │                    │                 │
  │ (Wait for R=2 responses)               │                 │
  │                   │ Response: {       │ Response:       │ Response:
  │                   │   value: "Alice", │   value: "Bob", │   value: "Alice",
  │                   │   clock: {N1:10}  │   clock: {N2:5} │   clock: {N1:10}
  │                   │ }                 │ }               │ }
  │                   │ ack1: ✓           │ ack2: ✓         │
  │                   │                    │                 │
  │                   │ Merge versions:    │                 │
  │                   │ {N1:10} vs {N2:5}  │                 │
  │                   │ N1:10 > N2:5 → AFTER (newer)        │
  │                   │                    │                 │
  │                   │ But we got both, so CONFLICT!        │
  │                   │                    │                 │
  │ Get response      │                    │                 │
  │ {found: true,    │                    │                 │
  │  versions: [     │                    │                 │
  │    {value: "Alice", clock: {N1:10}},  │                 │
  │    {value: "Bob", clock: {N2:5}}      │                 │
  │  ],               │                    │                 │
  │  message: "conflict: sibling versions"}                 │
  │ <──────────────────┤                    │                 │
  │
  │ Client application can:
  │ - Use last-write-wins (pick {N1:10})
  │ - Merge ("Alice" + "Bob")
  │ - Show to user (conflict resolution)
```

**Result**: Client gets both versions, knows there's a conflict.

**Why both versions exist**: The system didn't coordinate the merge because:
- Write from N1 went to N1, N2, N3
- Write from N2 went to only N2 (others didn't reach it)
- N3 kept N1's version
- No one knows the "true" order, so both are kept

---

### Example 3: Node Failure & Gossip Detection

**Setup**: 3-node cluster, N1 runs normally, then N2 crashes.

```
Time  N1 heartbeat  N2 heartbeat  N3 heartbeat  N2 Status
───────────────────────────────────────────────────────
  0      0             0              0          ALIVE
  1      1             1              1          ALIVE
  2      2             2              2          ALIVE
  3      3             ✗              3          ALIVE (1st miss)
  4      4             ✗              4          ALIVE (2nd miss)
  5      5             ✗              5          SUSPECT (3 cycles)
  6      6             ✗              6          SUSPECT (4 cycles)
  7      7             ✗              7          DEAD (6 cycles, removed)
  8      8             ✗              8          (not in ring anymore)
```

**Meanwhile, consistent hash ring updates**:

```
Step 1: N2 removed from ring
   Before:  prefs for "key" = [N1, N2, N3]
   After:   prefs for "key" = [N1, N3, ???] (only 2 nodes, replication factor=3)

Step 2: New writes happen
   Put("newkey", "value")
   Prefs: [N1, N3]
   (can only reach 2 of 3 replicas, but quorum W=2, so QUORUM still succeeds)

Step 3: N2 comes back
   Gossip detects N2 alive
   Anti-entropy repair compares N2 vs N1:
   - N2 missing "newkey"
   - Fetch from N1, apply to N2
   - N2 now caught up

Step 4: N2 added back to ring
   Prefs for "key" = [N1, N2, N3] again
```

---

## Testing Strategy

### 1. Unit Tests (What to Test)

**File**: Any file with `*Test.java` suffix

**Categories**:

| Component | What to test | Example test |
|-----------|--------------|--------------|
| **ConsistentHashRing** | Key distribution, node add/remove, preference list | `testDistributionUniformity()`, `testPreferenceListUnique()` |
| **VersionedValue** | Vector clock comparison, conflict detection | `testConcurrentWrites()`, `testVectorClockOrdering()` |
| **WALManager** | Write, replay, snapshot compaction | `testWALReplayRecovery()`, `testCompactionAndReplay()` |
| **BloomFilter** | False positive rate, insertion | `testFalsePositiveRate()` |
| **QuorumCoordinator** | Read/write quorum, timeout handling | `testQuorumWriteSuccess()`, `testReadConflict()` |
| **MembershipList** | Heartbeat increment, merge, failure detection | `testSuspectAfterMissedCycles()` |

**Example test structure**:

```java
@Test
void testQuorumWriteSuccessWithTwoOfThreeReplicas() {
  // Arrange
  QuorumCoordinator coordinator = new QuorumCoordinator(
    "node-1", ring, replicaClient, replicationFactor=3, timeout=1s
  );

  // Mock: 2 of 3 replicas respond
  when(replicaClient.write(N1, "key", value))
    .thenReturn(CompletableFuture.completedFuture(ok()));
  when(replicaClient.write(N2, "key", value))
    .thenReturn(CompletableFuture.completedFuture(ok()));
  when(replicaClient.write(N3, "key", value))
    .thenReturn(failedFuture(timeout()));

  // Act
  WriteQuorumResult result = coordinator.put("key", bytes, QUORUM);

  // Assert
  assertTrue(result.success());
  assertEquals(2, result.acksReceived());
}
```

### 2. Integration Tests

**What**: Test multiple layers together (without mocking).

**Example**: Start 3 real nodes, write with QUORUM, read, verify consistency.

```bash
# Start 3 local nodes with real gRPC
java -Dport=50051 ... &
java -Dport=50052 ... &
java -Dport=50053 ... &

# Run integration test
mvn test -Dgroups=integration

# Test: put("key", "value", QUORUM) → get("key", QUORUM)
```

### 3. Chaos Tests

**What**: Kill nodes, partition network, verify system recovers.

**Example test**: Chaos quorum test

```bash
cd deploy
docker compose up --build -d

# In another terminal:
./test/chaos/chaos-quorum.sh

# Script does:
# 1. Write burst to N1 with QUORUM
# 2. Kill N3 mid-burst
# 3. Continue writing with only 2 nodes
# 4. Verify quorum still works
# 5. Stop N2, write with ONE
# 6. Verify hinted handoff / anti-entropy kicks in
```

### 4. Load Tests

**What**: Measure throughput, latency, and behavior under load.

```bash
# Build the demo client
mvn -DskipTests package

# Run demo with load
java -cp target/distkv-0.1.0-SNAPSHOT.jar \
  com.distkv.client.DistKvDemoClient cluster

# Monitor with Prometheus/Grafana
# http://localhost:3000 (admin/admin)
```

---

## Running Demos

### Demo 1: Local Single Node

**Quick start**:
```bash
# Terminal 1: Start one node
java -jar target/distkv-0.1.0-SNAPSHOT.jar
# Listens on localhost:50051

# Terminal 2: Use grpcurl
grpcurl -plaintext \
  -d '{"key":"hello","value":"d29ybGQ=","consistency":"QUORUM"}' \
  localhost:50051 distkv.api.KVService/Put

grpcurl -plaintext \
  -d '{"key":"hello","consistency":"QUORUM"}' \
  localhost:50051 distkv.api.KVService/Get

# Or use Java client
java -cp target/distkv-0.1.0-SNAPSHOT.jar \
  com.distkv.client.DistKvDemoClient smoke
```

**What it shows**: Basic Put/Get works.

---

### Demo 2: Local 3-Node Cluster

**Setup**:
```bash
# Terminal 1: Node 1
DISTKV_NODE_ID=node-1 \
DISTKV_PORT=50051 \
java -jar target/distkv-0.1.0-SNAPSHOT.jar

# Terminal 2: Node 2
DISTKV_NODE_ID=node-2 \
DISTKV_HOST=localhost \
DISTKV_PORT=50052 \
DISTKV_PEERS=node-1:localhost:50051 \
java -jar target/distkv-0.1.0-SNAPSHOT.jar

# Terminal 3: Node 3
DISTKV_NODE_ID=node-3 \
DISTKV_HOST=localhost \
DISTKV_PORT=50053 \
DISTKV_PEERS=node-1:localhost:50051,node-2:localhost:50052 \
java -jar target/distkv-0.1.0-SNAPSHOT.jar
```

**Test quorum**:
```bash
# All 3 nodes are alive, write reaches all 3
java -cp target/distkv-0.1.0-SNAPSHOT.jar com.distkv.client.DistKvDemoClient cluster

# Check cluster status (before/after removing a node)
grpcurl -plaintext -d '{}' localhost:50051 distkv.api.AdminService/ClusterStatus
```

**What it shows**: 
- Consistent hashing maps keys across 3 nodes
- Quorum writes reach multiple replicas
- Gossip keeps nodes aware of each other

---

### Demo 3: Docker Compose (Full Stack)

**One command to start everything**:
```bash
cd deploy
docker compose up --build -d

# Services running:
# - localhost:50051 (node-1 gRPC)
# - localhost:50052 (node-2 gRPC)
# - localhost:50053 (node-3 gRPC)
# - localhost:9090 (Prometheus)
# - localhost:3000 (Grafana: admin/admin)
```

**Grafana dashboard**:
1. Go to http://localhost:3000
2. Login with admin/admin
3. Open the auto-provisioned DistKV dashboard
4. Watch metrics in real-time as you write/read

**Panels**:
- **Ops/sec**: How many get/put/delete per second
- **P99 latency**: 99th percentile latency (watch spikes when nodes fail)
- **Node health**: Heatmap showing which nodes are alive
- **Quorum failures**: Failed writes (should be 0 with healthy nodes)
- **WAL size**: Disk space per node

The dashboard JSON lives at `deploy/grafana-dashboard.json`; provisioning files under `deploy/grafana/provisioning` wire Grafana to Prometheus automatically.

**Cleanup**:
```bash
docker compose down -v
```

---

### Demo 4: Chaos - Kill Node Mid-Write

**Run the chaos test**:
```bash
cd deploy
docker compose up --build -d
cd ..

./test/chaos/chaos-quorum.sh
```

**What it does**:
1. Starts 3 nodes in Docker
2. Writes 100 keys to N1 with QUORUM
3. After 50 keys, kills N3 with `docker stop`
4. Continues writing with only 2 nodes
5. Verifies quorum succeeds (W=2, only 2 nodes left)
6. Reads a key back, verifies it's there
7. Brings N3 back
8. Verifies N3 catches up via anti-entropy

**Expected output**:
```
✓ Cluster started
✓ Writing with QUORUM (N1, N2, N3)...
✓ 50 keys written
✗ Killing N3
✓ Continuing writes with N1, N2 only
✓ 100 keys written (quorum still works!)
✓ Read key back: SUCCESS
✓ Bringing N3 back
✓ Waiting for anti-entropy repair...
✓ N3 caught up
✓ All 100 keys present on all 3 nodes
```

**What it demonstrates**:
- Quorum survives node failure
- Hinted handoff stores writes locally
- Anti-entropy repair heals divergence
- No single point of failure

---

### Demo 5: Test Bloom Filter Performance

**What**: See how Bloom filter avoids unnecessary lookups.

```bash
# Run this test
java -cp target/distkv-0.1.0-SNAPSHOT.jar com.distkv.storage.BloomFilterTest

# Expected output:
# Bloom filter false positive rate: ~1.0%
# (should be close to configured 1%)
```

---

## Interview Talking Points

### 1. "How does consistent hashing work?"

**Answer structure**:
- Map keys and nodes to a hash space (ring)
- Each node gets virtual tokens for better distribution
- To find replicas: hash key, walk clockwise, collect N distinct physical nodes
- Advantage: adding/removing nodes only reshuffles ~1/N of the keyspace
- Demo: show ConsistentHashRing distributes uniformly across virtual tokens

**Code pointer**: `ConsistentHashRing.java:60-80`

---

### 2. "Why is R + W > N important?"

**Answer structure**:
- N = total replicas (3)
- W = write quorum (2)
- R = read quorum (2)
- R + W > N (2+2 > 3) means read and write sets must overlap
- Overlap guarantees reads see latest write
- Demo: kill one node, show QUORUM still succeeds with 2/3

**Code pointer**: `QuorumCalculator.java`

---

### 3. "How do you handle concurrent writes?"

**Answer structure**:
- Use vector clocks: map of {nodeId → counter}
- Compare clocks to determine causality
- If CONCURRENT (neither < or >), keep both as siblings
- Client can use last-write-wins, merge, or ask user
- Demo: show conflict detection in get response

**Code pointer**: `VersionedValue.java:47-84`, `ClockRelation.java`

---

### 4. "How do you ensure durability?"

**Answer structure**:
- Write-ahead log: append to disk BEFORE updating memory
- On crash: reload from WAL
- Optimize with snapshots: take full snapshot every N writes, truncate WAL
- On restart: load snapshot + replay WAL delta (faster)
- Demo: kill a node mid-write, restart, verify data is there

**Code pointer**: `WALManager.java`

---

### 5. "How does failure detection work?"

**Answer structure**:
- Gossip protocol: each node pings random peers, exchanges membership lists
- If peer doesn't increment heartbeat for 3 cycles: SUSPECT
- If still no heartbeat for 3 more cycles: DEAD, remove from ring
- Advantage: no centralized coordinator, spreads info in O(log n) time
- Demo: docker stop one node, watch it go SUSPECT → DEAD in Grafana

**Code pointer**: `GossipService.java:63-78`, `MembershipList.java`

---

### 6. "How does data repair work?"

**Answer structure**:
- Hinted handoff: if replica is down, store write locally as "hint", deliver when replica recovers
- Anti-entropy: periodically compare replicas using Merkle tree, sync diverged keys
- Guarantees eventual consistency: all replicas eventually converge
- Demo: kill node, write more data, bring node back, show anti-entropy syncs it

**Code pointer**: `HintedHandoffManager.java`, `MerkleTree.java`, `AntiEntropyService.java`

---

### 7. "Why a Bloom filter?"

**Answer structure**:
- Bloom filter is a probabilistic data structure for membership testing
- Can answer "definitely not present" with certainty
- False positives possible, but false negatives impossible
- Space-efficient: ~120KB for 100k keys at 1% false positive rate
- Use case: skip expensive storage lookups for missing keys
- Demo: measure false positive rate matches configured value

**Code pointer**: `BloomFilter.java`, README Bloom Filter Math section

---

### 8. "What consistency levels are supported?"

**Answer structure**:
- ONE: fastest, least durable (read from any node, write to any node)
- QUORUM: balanced (read from majority, write to majority), default
- ALL: slowest, most durable (all replicas must participate)
- R + W > N guarantees consistency for QUORUM/ALL
- Demo: show different latencies for each level

**Code pointer**: `QuorumCalculator.java`, `ConsistencyLevel` enum in kv.proto

---

### 9. "How does membership change work?"

**Answer structure**:
- AdminService.NodeJoin: add new node, it gets virtual tokens on ring
- AdminService.NodeLeave: remove node, its keys redistribute
- Gossip detects node death automatically (don't need explicit leave)
- Ring update: future operations skip dead node
- Demo: docker stop node, show it disappears from ClusterStatus

**Code pointer**: `AdminServiceImpl.java`

---

### 10. "Why did you choose Java + gRPC?"

**Answer structure**:
- Java: well-known, mature ecosystem, excellent concurrency model
- gRPC: binary protocol (efficient), supports streaming, works on any platform
- Protobuf: type-safe serialization, code generation
- Netty: performant async I/O under the hood
- Demo: show performance metrics in Grafana

---

## Next Steps

1. **Read the code** in this order:
   - `kv.proto` - understand the wire format
   - `VersionedValue.java` - core data model
   - `ConsistentHashRing.java` - routing logic
   - `QuorumCoordinator.java` - orchestration
   - `WALManager.java` - durability
   - `GossipService.java` - membership

2. **Run the demos**:
   - Single node: verify Put/Get work
   - 3-node cluster: watch consistent hashing
   - Docker Compose: monitor with Prometheus/Grafana
   - Chaos test: see quorum survive failure

3. **Modify and experiment**:
   - Change replication factor from 3 to 5
   - Change consistency level from QUORUM to ONE, measure latency
   - Add a new metric (e.g., cache hit rate on Bloom filter)
   - Write a new chaos test (e.g., network partition)

4. **Interview preparation**:
   - Practice the talking points above
   - Be ready to draw the architecture diagram
   - Explain tradeoffs (consistency vs latency, N vs R+W)
   - Discuss failure modes (network partitions, Byzantine faults)

---

## Summary

**DistKV is a production-grade Dynamo-style distributed KV store that teaches**:

✓ Consistent hashing and routing
✓ Quorum replication and eventual consistency
✓ Vector clocks for conflict detection
✓ Write-ahead logging for durability
✓ Gossip protocols for membership
✓ Hinted handoff and anti-entropy repair
✓ Bloom filters for performance
✓ gRPC and Protocol Buffers for RPC
✓ Observability with Prometheus/Grafana
✓ Chaos testing for reliability

Each component solves a real problem in distributed systems. Together, they build a system that's:
- **Available**: survives node failures via replication
- **Partition-tolerant**: works across network partitions
- **Eventually consistent**: all replicas converge (AP in CAP theorem)

Good luck! 🚀
