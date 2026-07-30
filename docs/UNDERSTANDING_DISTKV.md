# Understanding DistKV — A Build-It-Up Learning Guide

> A second, independent walkthrough of this codebase. Where the existing
> `docs/images/guide/LEARNING_GUIDE.md` is organized as an SDE-2 interview reference
> (component-by-component), this guide teaches the system **from first principles**:
> we start with one node, then keep adding the *one missing thing* that forces the
> next feature into existence. Every section ends by pointing at the **real file and
> line** so you can read the source, not a paraphrase of it.

---

## 0. How to read this guide

There are two honest ways to learn a distributed system:

1. **Top-down** — start from the architecture diagram and drill in. (That's the other guide.)
2. **Bottom-up** — start from a `HashMap` and ask "what breaks if I have two of these on
   different machines?" Each answer is a feature. (That's this guide.)

I'll use bottom-up, because it makes every component feel *inevitable* instead of arbitrary.

**Suggested path:** read §1–§4 in order (they build on each other), then read §5–§12 in any
order. §13 (sharp edges) is the most valuable section once the rest makes sense — it's where
the design's real trade-offs and one genuine bug live.

**Conventions:** `file.java:NN` means look at that line. Run `mvn test` after each section that
mentions a test — the tests are tiny and are the fastest way to *prove* the behavior to yourself.

---

## 1. The core problem, and the smallest thing that works

A key-value store is morally a `Map<String, byte[]>`. If you only ever run one process and
never crash, you are done. Everything else in this repo exists to defend against exactly two
facts of life:

- **Machines die** (process crash, power loss, network partition).
- **One machine isn't enough** (capacity, throughput, blast radius).

The entire repo is the consequence of taking those two facts seriously. Hold that thought —
we'll add features one fact at a time.

The "smallest thing that works" lives in two classes:

- [`InMemoryKeyValueStore`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java) — the `Map`.
- [`KVServiceImpl`](../src/main/java/com/distkv/grpc/KVServiceImpl.java) — the network door (gRPC) in front of it.

If you deleted every other file and stubbed the replication out, you'd have a working
single-node store. Start your mental model there.

---

## 2. Fact #1: "machines die" → durability (WAL)

### The problem
Our `Map` is in RAM. A crash loses everything. We need writes to **survive a restart**.

### The idea
Before we change memory, we **append the change to a file on disk**. On restart we **replay**
the file to rebuild memory. This is a Write-Ahead Log (WAL). The cardinal rule:

> **Disk first, memory second.** Never acknowledge a write you haven't logged.

See it in [`InMemoryKeyValueStore.apply()`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L98):

```java
public synchronized void apply(String key, VersionedValue value) {
    appendToWal(key, value);   // 1. disk
    values.compute(...);       // 2. memory
    touch(key); addToBloom(key); evictIfNeeded(); compactWalIfNeeded();
}
```

If the process dies between line 1 and line 2, replay recovers the write. If you reversed the
order, a crash could lose an *acknowledged* write — the one thing a durable store must never do.

### The log grows forever — so we compact
A pure append-only log eventually fills the disk. The fix is a **snapshot**: periodically write
the current full state to a snapshot file, then truncate the log.
[`WALManager.compact()`](../src/main/java/com/distkv/storage/WALManager.java#L88) does this with
the classic *write-to-temp-then-atomic-rename* trick (`Files.move(..., REPLACE_EXISTING)`) so a
crash mid-compaction never leaves a half-written snapshot at the real path.

Recovery is therefore "load snapshot, then replay the leftover log on top":
[`WALManager.restore()`](../src/main/java/com/distkv/storage/WALManager.java#L78).

### What to take away
- The record format (magic number, version byte, then the value) is in
  [`WALManager.writeVersion`/`readVersion`](../src/main/java/com/distkv/storage/WALManager.java#L212).
  The magic/version bytes let recovery reject corrupt or incompatible files instead of loading garbage.
- **Honest limitation:** `append()` calls `flush()` but **not** `fsync`/`FileChannel.force()`.
  That protects you from a *process* crash but not from a *power* loss (the OS page cache can
  still lose the tail). That's a fine scope for a learning project — just don't claim power-loss durability.
- **Tests that prove it:** [`WALManagerTest`](../src/test/java/com/distkv/storage/WALManagerTest.java)
  and the recovery test in [`InMemoryKeyValueStoreTest`](../src/test/java/com/distkv/storage/InMemoryKeyValueStoreTest.java).

---

## 3. Fact #2: "one machine isn't enough" → where does a key live? (the ring)

Now we want N machines. The first question is brutally simple: **given a key, which machine(s)
store it?**

### Why not `hash(key) % N`?
Because the moment `N` changes (a node joins or dies), almost every key remaps to a different
node and the whole cluster reshuffles its data. That's a self-inflicted outage.

### Consistent hashing
Map both **nodes** and **keys** onto the same circular number line (0 … 2^128). A key belongs to
the first node you meet walking **clockwise** from the key's position. Now adding/removing a node
only disturbs the slice of the circle next to it — not the whole keyspace.

[`ConsistentHashRing`](../src/main/java/com/distkv/routing/ConsistentHashRing.java) implements this
with a `TreeMap` (a sorted map = the circle), so "walk clockwise" is `ring.tailMap(keyHash)` — an
`O(log n)` lookup, not a linear scan.

### Two subtleties the code gets right

1. **Virtual nodes** (`DEFAULT_VIRTUAL_NODES = 150`,
   [line 20](../src/main/java/com/distkv/routing/ConsistentHashRing.java#L20)). One physical node
   is placed at 150 random spots on the circle, not one. Why? With one spot per node, the circle
   is lumpy and some nodes own huge arcs (load skew). 150 spots average out the lumpiness. It also
   gives you a future knob: a beefier machine could get 300 tokens and naturally take more load.

2. **Distinct *physical* nodes in the preference list**
   ([`getPreferenceList` + `collectDistinctNodes`](../src/main/java/com/distkv/routing/ConsistentHashRing.java#L64)).
   When we want 3 replicas, we walk clockwise but **skip tokens that belong to a node we already
   picked** (the `seenPhysicalNodes` set). Without this, the 3 closest tokens might all be virtual
   copies of the *same* machine — "replication" onto one box, i.e. fake redundancy.

The ordered list of replicas for a key is its **preference list**. The first entry is special: it
is the key's **coordinator**.

> **Mental model:** the ring is a pure function `key -> [node, node, node]`. It has no I/O, no
> threads, no network. That's why it's trivially unit-testable
> ([`ConsistentHashRingTest`](../src/test/java/com/distkv/routing/ConsistentHashRingTest.java) checks
> that load is spread roughly evenly across nodes).

---

## 4. Trace one real request end-to-end

You now know enough to follow a `PUT` through the actual code. This is the single most useful
thing you can internalize.

### PUT("user:1", bytes, QUORUM)

1. **Client picks any healthy node** and sends `KVService/Put`
   ([`DistKvClient.put`](../src/main/java/com/distkv/client/DistKvClient.java#L62)). The client does
   *not* know the ring — it just talks to whoever is up.
2. **The contacted node asks: am I the coordinator for this key?**
   [`KVServiceImpl.put` → `remoteCoordinatorFor`](../src/main/java/com/distkv/grpc/KVServiceImpl.java#L177).
   - If **no**, it *forwards* the identical request to the real coordinator and relays the answer.
     This is why a client can be dumb: the server fixes up routing.
   - If **yes**, it calls the coordinator logic locally.
3. **Coordinator stamps a version** with a vector clock and fans out to all replicas:
   [`QuorumCoordinator.put` → `write`](../src/main/java/com/distkv/replication/QuorumCoordinator.java#L52).
   It sends to **all N** replicas in parallel (`CompletableFuture` per replica) but only **waits
   for `W` acknowledgements** (W depends on the consistency level — see §6).
4. **Each replica** runs `store.apply(...)` → WAL append → memory merge (§2).
   - If the replica is the coordinator itself, there's a **local fast path** that skips gRPC
     entirely: [`GrpcReplicaClient.write`](../src/main/java/com/distkv/replication/GrpcReplicaClient.java#L36)
     (`node.nodeId().equals(localEndpoint.nodeId())`).
5. **Count acks.** If `acks >= W`, return success. Any replica that *failed* gets a **hint** stored
   for later retry (§9) — but a hint does **not** count as an ack.

### GET is the mirror image
[`QuorumCoordinator.get`](../src/main/java/com/distkv/replication/QuorumCoordinator.java#L62): send
`ReplicaService/Read` to all replicas, collect everything that answers before the deadline,
**merge the versions by vector clock**, and succeed if `R` replicas responded.

Two design choices worth noticing while you read it:
- It fans out to **all** replicas and waits up to a single shared deadline, rather than returning
  the instant quorum is met. Simpler to reason about; slightly worse tail latency (a known
  trade-off — see §13).
- It does **no synchronous read-repair**. If it notices a stale replica during a read, it does
  *not* immediately fix it. Convergence is left to anti-entropy (§10).

> **Do this now:** open `KVServiceImpl.java` and `QuorumCoordinator.java` side by side and read
> `put` → `write` top to bottom. Everything else in this guide is detail hanging off this spine.

---

## 5. The thing that makes it a *distributed* store: versioned values

In a single-node map, "the value" is unambiguous. Across machines that can be partitioned, **two
clients can write the same key at the same time on different nodes**, and neither write saw the
other. Which one wins? Last-write-by-wall-clock is wrong, because clocks drift and you'd silently
lose data.

The answer this repo uses is the Dynamo answer: **keep a vector clock per value, and when two
versions are genuinely concurrent, keep both as siblings.**

[`VersionedValue`](../src/main/java/com/distkv/model/VersionedValue.java) carries:
`value bytes`, a `timestamp`, a `vectorClock` (`{nodeId -> counter}`), and a `tombstone` flag.

### The one rule you must understand: comparing vector clocks
[`compareVectorClock`](../src/main/java/com/distkv/model/VersionedValue.java#L61) returns one of
four relations:

| Relation | Meaning | Action on merge |
| --- | --- | --- |
| `AFTER` | A saw everything B saw, and more | A wins, drop B |
| `BEFORE` | A is an ancestor of B | B wins, drop A |
| `EQUAL` | identical clocks | tie-break on timestamp |
| `CONCURRENT` | each has something the other lacks | **keep both** |

Worked example:
```
{n1:3}          vs {n1:2}            -> AFTER      (3 ≥ 2, strictly greater somewhere)
{n1:3}          vs {n2:1}            -> CONCURRENT (each has a counter the other doesn't)
{n1:3, n2:1}    vs {n1:3}            -> AFTER      (superset)
```

The merge that applies this rule lives in **two** places that must agree:
[`QuorumCoordinator.mergeVersions`](../src/main/java/com/distkv/replication/QuorumCoordinator.java#L138)
(merging across replicas during a read) and
[`InMemoryKeyValueStore.mergeVersions`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L218)
(merging a new write into local state). Read both; they're intentionally near-identical.

### Deletes are just tombstones
You can't physically erase a key on delete, because a replica that missed the delete would later
"resurrect" the value during repair. So a delete is a special version with `tombstone=true`
([`VersionedValue.tombstone`](../src/main/java/com/distkv/model/VersionedValue.java#L27)). Reads
hide tombstones ([`get` filters them](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L115));
internal replication keeps them (`getVersionsIncludingTombstone`) so the deletion can propagate.
**Limitation:** there's no tombstone garbage collection, so deletes accumulate forever. Production
systems GC tombstones only after they're certain every replica has seen them.

> **Test that proves siblings:** the conflict/sibling cases in
> [`InMemoryKeyValueStoreTest`](../src/test/java/com/distkv/storage/InMemoryKeyValueStoreTest.java).

---

## 6. Quorum: turning "how many acks?" into a tunable knob

We send to N replicas. How many must answer before we call it a success? That single number is
the **consistency level**, and it's the heart of the system's CAP trade-off.

[`QuorumCalculator.requiredResponses`](../src/main/java/com/distkv/quorum/QuorumCalculator.java#L9):

| Level | Required (N=3) | You're buying | You're paying |
| --- | --- | --- | --- |
| `ONE` | 1 | lowest latency, highest availability | you might read stale data |
| `QUORUM` | `N/2 + 1` = 2 | the sweet spot (see below) | medium latency |
| `ALL` | N = 3 | strongest consistency | one slow node stalls you |

### Why QUORUM is magic: `R + W > N`
If your read quorum `R` and write quorum `W` satisfy `R + W > N`, then the set of nodes a read
touches and the set a write touched **must overlap by at least one node** (pigeonhole). That one
overlapping node has the latest write, so the read can't miss it. With N=3, `W=2` and `R=2` gives
`4 > 3` ✓. That's the whole reason QUORUM/QUORUM gives you read-your-writes without the cost of ALL.

This property is encoded literally in
[`QuorumCalculator.readWriteOverlap`](../src/main/java/com/distkv/quorum/QuorumCalculator.java#L20)
and checked in [`QuorumCalculatorTest`](../src/test/java/com/distkv/quorum/QuorumCalculatorTest.java).

### One sharp detail to file away
The coordinator computes the required count from `replicas.size()`, **not** the configured
replication factor:
```java
int requiredAcks = QuorumCalculator.requiredResponses(replicas.size(), consistencyLevel);
```
If a node has been evicted from the ring and only 2 replicas exist, QUORUM now means "2 of 2."
This keeps the system *available* during failures (great for a demo) but it means the strict
`R + W > original N` guarantee is computed against the *current* replica count. Know this; it's a
classic interview gotcha and it's a real semantic choice, not an accident (§13 revisits it).

---

## 7. The network layer: three gRPC services for three audiences

All wire types are defined once in [`kv.proto`](../src/main/proto/kv.proto) and code-generated at
build time (the `protobuf-maven-plugin` in `pom.xml`). There are deliberately **three** services
because there are three different callers:

| Service | Who calls it | RPCs |
| --- | --- | --- |
| `KVService` | external clients | `Get`, `Put`, `Delete`, `Scan` (server-streaming) |
| `AdminService` | operators / membership | `ClusterStatus`, `NodeJoin`, `NodeLeave` |
| `ReplicaService` | **other nodes** (internal) | `Apply`, `ApplyVersions`, `Read`, `Merkle`, `FetchVersions` |

The split matters: a client should never call `ReplicaService.Apply` directly (it bypasses
quorum). Keeping the internal data-plane in a separate service makes that boundary explicit.

Protobuf objects are *transport* types; the system logic uses Java domain types
(`VersionedValue`, `NodeEndpoint`). [`ProtoMappers`](../src/main/java/com/distkv/grpc/ProtoMappers.java)
is the single conversion seam — keeping the protobuf dependency from leaking into the storage and
replication layers.

> **Note on `Scan`:** it carries a `ConsistencyLevel` field for API symmetry, but the
> implementation is a **local-only** range read from whichever node you contacted
> ([`KVServiceImpl.scan`](../src/main/java/com/distkv/grpc/KVServiceImpl.java#L136)). It does *not*
> gather ranges from across the ring. A real distributed scan needs token-range ownership,
> pagination, and cross-replica merge — explicitly out of scope here, and the code comments say so.

---

## 8. Membership: who is even alive? (gossip + failure detection)

The ring is only useful if it reflects reality. When a node dies, it must leave the ring so new
operations stop targeting it. But there's **no central coordinator** to declare deaths — so nodes
figure it out among themselves.

### The model
Each node holds a [`MembershipList`](../src/main/java/com/distkv/membership/MembershipList.java) of
`{node -> heartbeat, lastSeen, status}` where status is `ALIVE → SUSPECT → DEAD`
([`MemberStatus`](../src/main/java/com/distkv/membership/MemberStatus.java)).

### The loop
Every second ([`GossipService.gossipOnce`](../src/main/java/com/distkv/membership/GossipService.java#L63)):
1. Bump my own heartbeat counter.
2. Pick a small **random fanout** of peers (default 2).
3. Exchange membership with them and merge what I learn
   ([`MembershipList.merge`](../src/main/java/com/distkv/membership/MembershipList.java#L73): higher
   heartbeat wins for liveness; a recent enough `DEAD` can propagate).
4. Mark anyone I haven't heard about recently as `SUSPECT`, then `DEAD`
   ([`markFailures`](../src/main/java/com/distkv/membership/MembershipList.java#L112)): SUSPECT after
   3 stale cycles, DEAD after 6.

When a node is declared DEAD, a callback removes it from the ring. Wired in
[`DistKvServer`](../src/main/java/com/distkv/server/DistKvServer.java#L95):
```java
member -> ring.removeNode(member.endpoint().nodeId())
```

### Be honest about what "gossip" means here
The interface [`GossipPeerClient.exchange(peer, localMembership)`](../src/main/java/com/distkv/membership/GossipPeerClient.java)
*looks* like a bidirectional push-pull exchange, but the gRPC implementation
([`GrpcGossipPeerClient`](../src/main/java/com/distkv/membership/GrpcGossipPeerClient.java#L25))
**ignores the `localMembership` argument** and simply *pulls* the peer's `ClusterStatus`. So this is
"randomized **pull** of full cluster status," not true push-pull gossip. Propagation still works
(everyone pulls everyone over time), but the `localMembership` parameter is currently dead weight,
and liveness only spreads in the direction someone happens to pull. Good thing to call out if you
present this project.

### Why eventual, not instant
A failure detector based on missed heartbeats is **eventually accurate**, never instantly accurate.
A GC pause or a brief network blip can make a healthy node look SUSPECT. That's inherent — the
tunables (cycles-to-suspect/dead) just trade detection speed against false-positive rate.

---

## 9. Hinted handoff: surviving *short* outages without losing the write

Scenario: a write needs to reach replicas {A, B, C}. C is briefly down. QUORUM is still satisfied
by A and B, so the client gets success — but **C is now stale** and nothing has told it so.

The fix: when a replica write fails, the coordinator stashes a **hint** — "deliver this write to C
when C comes back."
[`QuorumCoordinator.write`](../src/main/java/com/distkv/replication/QuorumCoordinator.java#L123)
calls `hintedHandoffManager.storeHint(...)` on each failed attempt, and
[`HintedHandoffManager.deliverHints`](../src/main/java/com/distkv/replication/HintedHandoffManager.java#L59)
retries every 2 seconds until C acknowledges.

One neat detail: the delivery loop snapshots the queue length *first*
(`int attempts = hints.size();`) so that hints which fail and get **re-enqueued** wait for the next
pass instead of spinning in a hot loop.

**Limitations (all genuine, all worth saying out loud):**
- Hints live in **memory only** and the queue is **unbounded** — a long outage under heavy writes
  grows it without limit, and a coordinator crash loses every pending hint.
- That's exactly why hinted handoff is **not enough on its own** — which motivates §10.

The metric `distkv_pending_hints` and `distkv_replication_lag_ms` (really "age of the oldest
pending hint", see [`oldestHintAgeMillis`](../src/main/java/com/distkv/replication/HintedHandoffManager.java#L52))
let you watch this backlog drain.

---

## 10. Anti-entropy: guaranteeing convergence after *long* outages

Hinted handoff handles seconds of downtime. What about a node that was down for an hour, or a
coordinator that crashed with undelivered hints, or a write that quietly failed on one replica?
For those, replicas need a way to **find and fix divergence on their own, forever, in the
background.** That's anti-entropy.

### The trick: compare hashes, not data
Sending your entire dataset to a peer to compare is wasteful. Instead each side builds a tree of
hashes (a **Merkle tree**). If the **root hashes match**, the datasets are identical and you're
done in one comparison. If they differ, you descend to find exactly which keys differ.

[`AntiEntropyService.repairOnce`](../src/main/java/com/distkv/repair/AntiEntropyService.java#L49),
every 30s by default:
1. Build my Merkle tree from local state.
2. For each peer: fetch its tree. If roots match, skip.
3. Otherwise compute `differingKeys` and, for each, fetch the peer's versions, apply them locally,
   then push my merged versions back — **bidirectional** repair
   ([`repairPeer`](../src/main/java/com/distkv/repair/AntiEntropyService.java#L72)).

### Read the implementation honestly
[`MerkleTree`](../src/main/java/com/distkv/repair/MerkleTree.java) is named "tree" but is really a
**flat map of per-key leaf hashes plus one root hash over all leaves**
([`differingKeys`](../src/main/java/com/distkv/repair/MerkleTree.java#L40) just diffs the two leaf
maps). A real Merkle tree would let you binary-search ranges so you transfer `O(log n)` hashes for a
small diff; this version sends **all** leaf hashes every time. Fine for learning, wrong at scale.

Also note: [`ReplicaServiceImpl.merkle`](../src/main/java/com/distkv/grpc/ReplicaServiceImpl.java#L78)
**ignores** the `start_key`/`end_key` range in the request and always hashes the whole store, and
the peer list in `DistKvServer` is "all ring nodes except me." With replication factor = cluster
size (the default 3/3 demo) that's harmless, but with `RF < clusterSize` it would happily copy keys
to nodes that aren't even in the key's preference list — spreading data beyond its intended replicas.
See §13.

Leaf hashes deliberately include the value, timestamp, tombstone flag, **and** sorted vector-clock
entries ([`hashLeaf`](../src/main/java/com/distkv/repair/MerkleTree.java#L57)), because two replicas
only truly "agree" on a key when their full version sets match — not just the latest visible value.

> **Test:** [`MerkleTreeTest`](../src/test/java/com/distkv/repair/MerkleTreeTest.java) checks that a
> divergent key is detected.

---

## 11. The read-path optimizations: Bloom filter + LRU

Two storage-engine classics, both optional (the store works without them).

### Bloom filter — "definitely not here" in O(1)
A [`BloomFilter`](../src/main/java/com/distkv/storage/BloomFilter.java) answers one question fast:
*could* this key exist? It can say **"definitely not"** (skip the lookup entirely) or **"maybe"**
(go check). It never produces a false negative for a key that was inserted, but it can produce false
positives. The store checks it first in
[`isDefinitelyAbsent`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L249).

The sizing math (`m` bits, `k` hash functions) is in
[`calculateBitSize`/`calculateHashCount`](../src/main/java/com/distkv/storage/BloomFilter.java#L64),
and the implementation uses **double hashing** off a single SHA-256 digest
([`indexesFor`](../src/main/java/com/distkv/storage/BloomFilter.java#L72)) — a standard way to
synthesize `k` hash functions from two. `BloomFilterTest` asserts the bit/hash counts and that the
false-positive rate stays near the target.

### LRU eviction — bounding memory
The store can cap memory by **entry count** (`DISTKV_MAX_ENTRIES`) or **approximate bytes**
(`DISTKV_MAX_MEMORY_BYTES`). Access order is tracked by a `LinkedHashMap(16, 0.75f, true)` — the
`true` turns on access-order, so iterating gives least-recently-used first
([field](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L23),
[`evictIfNeeded`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L268)).

**A subtlety worth understanding deeply:** evicting here means *dropping the key from serving
memory*, but the write is **still in the WAL**. So after eviction a `GET` can return "not found"
for a key that was successfully written and acked — the data isn't lost (restart/repair can bring it
back) but it's not being served. For a *cache* that's correct; for a *database* it's surprising.
The repo blurs "cache eviction" and "data deletion," which is fine for learning as long as you can
articulate the difference. (This only bites when you set a very small cap.)

---

## 12. Observability and how it's wired together

[`DistKvMetrics`](../src/main/java/com/distkv/observability/DistKvMetrics.java) exposes a Prometheus
endpoint per node. Two patterns to notice:

- **Latency + count in one wrapper:**
  [`recordOperation`](../src/main/java/com/distkv/observability/DistKvMetrics.java#L73) starts a
  histogram timer, runs the operation in a `try/finally`, and increments the op counter. Every KV
  handler is wrapped in this via `KVServiceImpl.record(...)`.
- **Periodic gauges:** [`DistKvServer`](../src/main/java/com/distkv/server/DistKvServer.java#L126)
  schedules a 5-second task to publish node health, WAL size, pending hints, and hint age. Note the
  `catch (Exception ignored)` — metrics reporting must never crash request handling.

The metrics map directly onto the 5 Grafana panels (ops/sec, P99 latency, node-health heatmap,
quorum failures, WAL size). The dashboard JSON and Prometheus/Grafana provisioning are under
`deploy/`. P99 matters specifically because quorum systems are **tail-latency sensitive** — you wait
on the slowest of the replicas you need.

### The full wiring, in one place
The best single file to cement the whole architecture is
[`DistKvServer.main`](../src/main/java/com/distkv/server/DistKvServer.java#L41). It is plain
constructor dependency injection (no framework): read env → build ring → build WAL+store and recover
→ start metrics → build membership and add peers → build replica client + hinted handoff + coordinator
→ start gossip + anti-entropy → start the gRPC server with the three services → announce join →
start the metrics reporter → register a shutdown hook that closes everything in order. Read it once
top to bottom and the dependency graph clicks. Also note: gRPC handlers run on
`Executors.newVirtualThreadPerTaskExecutor()` (Java 21 virtual threads), which is a great fit because
handlers block on quorum futures and WAL appends.

---

## 13. Sharp edges — bugs, gaps, and things that are subtler than they look

This is the section to actually study. A demo distributed system earns credibility by being honest
about its limits. I verified each item below against the source.

### 🔴 Real bug: writes can be silently lost after a coordinator restart
This is the one genuine correctness defect, and it's subtle.
*(Fixed on branch `fix/restart-counter-and-deadcode`: the coordinator now re-seeds its counter
from `InMemoryKeyValueStore.recoveredCounterFor(nodeId)` at startup. The walkthrough below
describes the original defect so the reasoning is preserved.)*

- The coordinator stamps every write with a vector clock of just `{coordinatorId : counter}`, where
  `counter` comes from an in-memory `AtomicLong` that **starts at 0** and is **never restored from
  the WAL**: [`QuorumCoordinator` field + put](../src/main/java/com/distkv/replication/QuorumCoordinator.java#L30).
- On restart, WAL recovery restores the *old* versions (which may carry a high counter like
  `{node-1: 8000}`), but the new `AtomicLong` begins again at 1.
- New writes therefore carry **lower** counters (`{node-1: 1}`, `{node-1: 2}`, …). The merge logic
  compares them to the restored `{node-1: 8000}`, sees `BEFORE`, and **discards the new write as
  stale** — while the replica still returns "applied," so the **client sees success**.
- Net effect: after a restart, writes to any previously-written key are silently dropped until the
  counter climbs back above that key's stored counter.

Why it exists: the store *has* a causal-merge write path
([`InMemoryKeyValueStore.put`](../src/main/java/com/distkv/storage/InMemoryKeyValueStore.java#L76)
calls `mergeVectorClocks`, which would increment *on top of* the existing clock and avoid this), but
**the live request path never uses it** — the coordinator builds a bare clock and calls
`store.apply` instead. So `KeyValueStore.put`/`delete` are effectively test-only, and the production
path bypasses the very logic that would prevent the bug.

**Fixes (any one):** (a) during WAL recovery, seed `localCounter` to the max counter seen for this
node id; (b) have the coordinator read the current version and merge/increment its clock instead of
starting fresh; or (c) persist the counter alongside the WAL. Option (a) is the smallest change.

### 🟠 Coordinator has no failover
The coordinator for a key is strictly preference-list[0]. If a client contacts a healthy node but
the *coordinator* is down (and gossip hasn't evicted it yet), the forward in
[`KVServiceImpl`](../src/main/java/com/distkv/grpc/KVServiceImpl.java#L177) fails, and retrying other
nodes just forwards to the same dead coordinator until gossip removes it (up to ~6s). A more robust
design would fail over to the next replica as coordinator.

### 🟠 The chaos test can abort during the detection window
[`test/chaos/chaos-quorum.sh`](../test/chaos/chaos-quorum.sh) stops node-3 mid-burst and keeps
writing. For keys whose **coordinator** is node-3, the forward fails until gossip evicts node-3;
`grpcurl` has no retry and the script runs under `set -e`, so an unlucky write can abort the whole
script. It usually passes, but it's timing-dependent rather than robust. (The *protocol* is fine —
QUORUM with 2/3 alive works; it's the test harness that's fragile.)

### 🟠 Anti-entropy isn't preference-list-aware
As noted in §10, the Merkle RPC ignores its key-range arguments and repair targets all ring nodes.
Safe only because the default demo runs `RF = clusterSize = 3`. With `RF < clusterSize` it would
replicate keys onto nodes outside their preference list.

### 🟡 "Gossip" is pull-only
The `localMembership` argument to `GossipPeerClient.exchange` is ignored by the gRPC implementation
(§8). Not a bug, but the API over-promises and liveness only propagates in the pull direction.

### 🟡 Durability is process-crash only, not power-loss
No `fsync` in the WAL (§2). Fine for the stated scope; just don't claim more.

### 🟡 No tombstone GC, no client-supplied causal context
Deletes accumulate forever (§5), and the public `Put` API doesn't accept a vector clock, so clients
can't explicitly resolve siblings the way real Dynamo clients do (read siblings → write with the
merged context). The system *detects* and *retains* conflicts but offers no public resolution path.

### Dead / unused code (removed on branch `fix/restart-counter-and-deadcode`)
I grep-verified each of these had **zero callers** anywhere (neither `src/main` nor `src/test`)
before deleting them:

| Symbol | File | Note |
| --- | --- | --- |
| `MembershipList.markSuspects(...)` | `membership/MembershipList.java:100` | superseded by `markFailures`; only mentioned in a comment |
| `WALManager.replayWalLatestOnly()` | `storage/WALManager.java:128` | private, never called |
| `InMemoryKeyValueStore.sortedKeys()` | `storage/InMemoryKeyValueStore.java:188` | unused |
| `InMemoryKeyValueStore.size()` | `storage/InMemoryKeyValueStore.java:192` | unused |
| `KeyValueStore.snapshot()` (+ impl) | `storage/KeyValueStore.java` | the single-version map; only `snapshotVersions()` is used |
| `VersionedValue.visible(...)` | `model/VersionedValue.java:112` | unused |
| `BloomFilter.expectedInsertions()` / `configuredFalsePositiveRate()` | `storage/BloomFilter.java` | unused getters |
| `ConsistentHashRing.virtualNodes()` | `routing/ConsistentHashRing.java:91` | unused getter |
| `WALManager.walPath()` | `storage/WALManager.java:120` | unused (`snapshotPath()` is used by a test) |

**Test-only but legitimate (keep them):** `WALManager.replay()`, `ConsistentHashRing.getCoordinator()`,
`QuorumCalculator.readWriteOverlap()`, `BloomFilter.bitSize()/hashCount()`,
`InMemoryKeyValueStore.put/delete` (counter-merge variants), `LocalReplicaClient`, and the several
convenience constructors. These are test seams or proofs-of-property, not waste.

---

## 14. A hands-on lab (do these in order)

1. **Prove durability.** Run `mvn test` and read `WALManagerTest`. Then in a scratch `main`, write a
   key, "crash" (drop the store reference), build a new store on the same data dir, call
   `recoverFromWal()`, and read the key back.
2. **Prove the ring rebalances cheaply.** In `ConsistentHashRingTest`, add a 4th node and assert that
   only a small fraction of keys change owner.
3. **See a sibling.** Apply two `VersionedValue`s with concurrent clocks (`{n1:1}` and `{n2:1}`) for
   the same key and assert `getVersions` returns 2.
4. **Run the cluster.** `cd deploy && docker compose up --build -d`, then use the `grpcurl` examples
   in the README. Open Grafana at `localhost:3000` (admin/admin) and watch ops/sec while you write.
5. **Break it on purpose.** Run `./test/chaos/chaos-quorum.sh` and watch `distkv_pending_hints` climb
   on the survivors, then drain when node-3 restarts.
6. **Reproduce the restart bug from §13.** Single node: write key `k` ~20 times (counter climbs),
   confirm the value; restart the node (so the `AtomicLong` resets but the WAL restores the high
   clock); write a *new* value to `k`; observe the read still returns the old value even though the
   PUT reported success. Then implement fix (a) and watch it pass.

---

## 15. Glossary (one line each)

- **Coordinator** — the first node in a key's preference list; orchestrates that key's reads/writes.
- **Preference list** — the ordered set of N nodes responsible for a key.
- **Vector clock** — `{node -> counter}` capturing causality; lets us tell "newer" from "concurrent."
- **Sibling** — one of multiple concurrent versions kept because no clock dominates the others.
- **Tombstone** — a delete marker; a version that says "the latest state is deleted."
- **Quorum** — the minimum acks/responses required; `R + W > N` guarantees read/write overlap.
- **Hinted handoff** — stash-and-retry of a write that a replica missed during a short outage.
- **Anti-entropy** — background convergence by comparing Merkle hashes and syncing differing keys.
- **WAL** — write-ahead log; the disk record written *before* memory so crashes don't lose acks.
- **Bloom filter** — probabilistic "definitely-absent / maybe-present" set for skipping lookups.

---

## 16. If you only remember five things

1. **Disk before memory** is the durability invariant; everything else is recovery detail.
2. **Consistent hashing** exists so membership changes move *some* keys, not *all* keys.
3. **`R + W > N`** is why QUORUM gives you consistency without paying for ALL.
4. **Vector clocks** let the system distinguish "stale" from "concurrent," and concurrent writes are
   *kept*, not silently dropped.
5. **Three layers of healing** stack up by outage length: quorum (instant) → hinted handoff
   (seconds) → anti-entropy (eventual).

And the meta-lesson from §13: a distributed system is defined as much by the failure modes it
*admits* as by the features it advertises.
