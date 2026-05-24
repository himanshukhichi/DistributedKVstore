package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.proto.ConsistencyLevel;
import com.distkv.routing.ConsistentHashRing;
import com.distkv.storage.InMemoryKeyValueStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumCoordinatorTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC);

    @Test
    void quorumWriteAndReadSucceedAcrossReplicas() {
        TestCluster cluster = TestCluster.withNodes(3, clock);
        QuorumCoordinator coordinator = cluster.coordinator("node-0", 3);

        WriteQuorumResult write = coordinator.put("alpha", "value".getBytes(), ConsistencyLevel.QUORUM);
        ReadQuorumResult read = coordinator.get("alpha", ConsistencyLevel.QUORUM);

        assertTrue(write.success());
        assertTrue(read.success());
        assertTrue(read.value().isPresent());
        assertArrayEquals("value".getBytes(), read.value().get().value());
    }

    @Test
    void quorumWriteFailsWhenTooFewReplicasAcknowledge() {
        TestCluster cluster = TestCluster.withNodes(3, clock);
        LocalReplicaClient client = new LocalReplicaClient();
        List<NodeEndpoint> replicas = cluster.ring().getPreferenceList("alpha", 3);
        client.register(replicas.get(0), new InMemoryKeyValueStore(clock));
        QuorumCoordinator coordinator = new QuorumCoordinator(
                "node-0",
                cluster.ring(),
                client,
                3,
                Duration.ofSeconds(1),
                clock);

        WriteQuorumResult write = coordinator.put("alpha", "value".getBytes(), ConsistencyLevel.QUORUM);

        assertFalse(write.success());
    }

    private record TestCluster(ConsistentHashRing ring, LocalReplicaClient client) {
        static TestCluster withNodes(int nodeCount, Clock clock) {
            ConsistentHashRing ring = new ConsistentHashRing(64);
            LocalReplicaClient client = new LocalReplicaClient();
            for (int index = 0; index < nodeCount; index++) {
                NodeEndpoint node = new NodeEndpoint("node-" + index, "localhost", 5000 + index);
                ring.addNode(node);
                client.register(node, new InMemoryKeyValueStore(clock));
            }
            return new TestCluster(ring, client);
        }

        QuorumCoordinator coordinator(String nodeId, int replicationFactor) {
            return new QuorumCoordinator(
                    nodeId,
                    ring,
                    client,
                    replicationFactor,
                    Duration.ofSeconds(1),
                    Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC));
        }
    }
}
