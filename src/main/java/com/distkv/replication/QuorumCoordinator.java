package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.model.ClockRelation;
import com.distkv.proto.ConsistencyLevel;
import com.distkv.quorum.QuorumCalculator;
import com.distkv.routing.ConsistentHashRing;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public final class QuorumCoordinator {
    private final String coordinatorNodeId;
    private final ConsistentHashRing ring;
    private final ReplicaClient replicaClient;
    private final int replicationFactor;
    private final Duration timeout;
    private final Clock clock;
    private final HintedHandoffManager hintedHandoffManager;
    private final AtomicLong localCounter = new AtomicLong();

    public QuorumCoordinator(String coordinatorNodeId, ConsistentHashRing ring, ReplicaClient replicaClient,
                             int replicationFactor, Duration timeout, Clock clock) {
        this(coordinatorNodeId, ring, replicaClient, replicationFactor, timeout, clock, null);
    }

    public QuorumCoordinator(String coordinatorNodeId, ConsistentHashRing ring, ReplicaClient replicaClient,
                             int replicationFactor, Duration timeout, Clock clock,
                             HintedHandoffManager hintedHandoffManager) {
        this.coordinatorNodeId = Objects.requireNonNull(coordinatorNodeId, "coordinatorNodeId");
        this.ring = Objects.requireNonNull(ring, "ring");
        this.replicaClient = Objects.requireNonNull(replicaClient, "replicaClient");
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be positive");
        }
        this.replicationFactor = replicationFactor;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hintedHandoffManager = hintedHandoffManager;
    }

    public WriteQuorumResult put(String key, byte[] value, ConsistencyLevel consistencyLevel) {
        VersionedValue version = VersionedValue.put(value, coordinatorNodeId, localCounter.incrementAndGet(), clock);
        return write(key, version, consistencyLevel);
    }

    public WriteQuorumResult delete(String key, ConsistencyLevel consistencyLevel) {
        VersionedValue version = VersionedValue.tombstone(coordinatorNodeId, localCounter.incrementAndGet(), clock);
        return write(key, version, consistencyLevel);
    }

    public ReadQuorumResult get(String key, ConsistencyLevel consistencyLevel) {
        List<NodeEndpoint> replicas = replicasFor(key);
        if (replicas.isEmpty()) {
            return new ReadQuorumResult(false, 0, List.of(), "no replicas available for key");
        }
        int requiredResponses = QuorumCalculator.requiredResponses(replicas.size(), consistencyLevel);
        List<CompletableFuture<ReplicaReadResult>> futures = replicas.stream()
                .map(node -> replicaClient.read(node, key))
                .toList();

        int responses = 0;
        List<VersionedValue> mergedVersions = List.of();
        long deadline = System.nanoTime() + timeout.toNanos();
        for (CompletableFuture<ReplicaReadResult> future : futures) {
            Optional<ReplicaReadResult> result = await(future, deadline);
            if (result.isEmpty() || !result.get().responded()) {
                continue;
            }
            responses++;
            for (VersionedValue version : result.get().versions()) {
                mergedVersions = mergeVersions(mergedVersions, version);
            }
        }

        boolean quorumReached = responses >= requiredResponses;
        if (!quorumReached) {
            return new ReadQuorumResult(false, responses, List.of(),
                    "read quorum failed: required " + requiredResponses + " responses but received " + responses);
        }
        VersionedValue latest = VersionedValue.latestIncludingTombstone(mergedVersions);
        if (latest == null || latest.tombstone()) {
            return new ReadQuorumResult(true, responses, mergedVersions, "not found");
        }
        return new ReadQuorumResult(true, responses, mergedVersions,
                mergedVersions.size() > 1 ? "conflict: returned sibling versions" : "found");
    }

    public List<NodeEndpoint> replicasFor(String key) {
        return ring.getPreferenceList(key, replicationFactor);
    }

    public Optional<NodeEndpoint> coordinatorFor(String key) {
        return replicasFor(key).stream().findFirst();
    }

    private WriteQuorumResult write(String key, VersionedValue version, ConsistencyLevel consistencyLevel) {
        List<NodeEndpoint> replicas = replicasFor(key);
        if (replicas.isEmpty()) {
            return new WriteQuorumResult(false, 0, 0, "no replicas available for key", version);
        }
        int requiredAcks = QuorumCalculator.requiredResponses(replicas.size(), consistencyLevel);
        List<ReplicaWriteAttempt> attempts = replicas.stream()
                .map(node -> new ReplicaWriteAttempt(node, replicaClient.write(node, key, version)))
                .toList();

        int acks = 0;
        long deadline = System.nanoTime() + timeout.toNanos();
        for (ReplicaWriteAttempt attempt : attempts) {
            Optional<ReplicaWriteResult> result = await(attempt.future(), deadline);
            if (result.isPresent() && result.get().acknowledged()) {
                acks++;
            } else if (hintedHandoffManager != null) {
                hintedHandoffManager.storeHint(attempt.node(), key, version);
            }
        }
        boolean success = acks >= requiredAcks;
        String message = success
                ? "write quorum satisfied"
                : "write quorum failed: required " + requiredAcks + " acks but received " + acks;
        return new WriteQuorumResult(success, requiredAcks, acks, message, version);
    }

    private List<VersionedValue> mergeVersions(List<VersionedValue> current, VersionedValue incoming) {
        if (current.isEmpty()) {
            return List.of(incoming);
        }
        List<VersionedValue> merged = new ArrayList<>();
        boolean incomingSuperseded = false;
        for (VersionedValue existing : current) {
            ClockRelation relation = incoming.compareVectorClock(existing);
            if (relation == ClockRelation.AFTER) {
                continue;
            }
            if (relation == ClockRelation.BEFORE) {
                incomingSuperseded = true;
                merged.add(existing);
                continue;
            }
            if (relation == ClockRelation.EQUAL) {
                merged.add(incoming.isNewerThan(existing) ? incoming : existing);
                incomingSuperseded = true;
                continue;
            }
            merged.add(existing);
        }
        if (!incomingSuperseded) {
            merged.add(incoming);
        }
        return List.copyOf(merged);
    }

    private <T> Optional<T> await(CompletableFuture<T> future, long deadlineNanos) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(future.get(remainingNanos, TimeUnit.NANOSECONDS));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (ExecutionException | TimeoutException exception) {
            return Optional.empty();
        }
    }

    private record ReplicaWriteAttempt(NodeEndpoint node, CompletableFuture<ReplicaWriteResult> future) {
    }
}
