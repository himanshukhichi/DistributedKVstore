package com.distkv.routing;

import com.distkv.model.NodeEndpoint;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {
    @Test
    void preferenceListReturnsDistinctPhysicalNodesInRingOrder() {
        ConsistentHashRing ring = new ConsistentHashRing(64);
        ring.addNode(new NodeEndpoint("node-a", "localhost", 5001));
        ring.addNode(new NodeEndpoint("node-b", "localhost", 5002));
        ring.addNode(new NodeEndpoint("node-c", "localhost", 5003));

        List<NodeEndpoint> preferenceList = ring.getPreferenceList("customer:123", 3);

        assertEquals(3, preferenceList.size());
        assertEquals(3, preferenceList.stream().map(NodeEndpoint::nodeId).distinct().count());
    }

    @Test
    void distributionIsReasonablyUniformWithVirtualNodes() {
        ConsistentHashRing ring = new ConsistentHashRing(150);
        for (int index = 0; index < 5; index++) {
            ring.addNode(new NodeEndpoint("node-" + index, "localhost", 5000 + index));
        }

        Map<String, Integer> counts = new HashMap<>();
        int keyCount = 20_000;
        for (int index = 0; index < keyCount; index++) {
            NodeEndpoint coordinator = ring.getCoordinator("key-" + index).orElseThrow();
            counts.merge(coordinator.nodeId(), 1, Integer::sum);
        }

        int ideal = keyCount / ring.size();
        counts.values().forEach(count -> {
            double skew = Math.abs(count - ideal) / (double) ideal;
            assertTrue(skew < 0.25, "skew was " + skew + " for count " + count);
        });
    }

    @Test
    void removingNodeEliminatesItFromPreferenceLists() {
        ConsistentHashRing ring = new ConsistentHashRing(32);
        ring.addNode(new NodeEndpoint("node-a", "localhost", 5001));
        ring.addNode(new NodeEndpoint("node-b", "localhost", 5002));

        assertTrue(ring.removeNode("node-a"));

        List<NodeEndpoint> preferenceList = ring.getPreferenceList("key", 2);
        assertFalse(preferenceList.stream().anyMatch(node -> node.nodeId().equals("node-a")));
        assertEquals(1, preferenceList.size());
    }
}
