package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.storage.InMemoryKeyValueStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HintedHandoffManagerTest {
    @Test
    void keepsHintUntilReplicaBecomesReachable() {
        Clock clock = Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC);
        NodeEndpoint node = new NodeEndpoint("node-b", "localhost", 5002);
        LocalReplicaClient replicaClient = new LocalReplicaClient();
        HintedHandoffManager manager = new HintedHandoffManager(replicaClient, Duration.ofSeconds(1));
        VersionedValue value = new VersionedValue("value".getBytes(), clock.millis(), Map.of("node-a", 1L), false);

        manager.storeHint(node, "alpha", value);
        manager.deliverHints();

        assertEquals(1, manager.pendingHintCount());

        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock);
        replicaClient.register(node, store);
        manager.deliverHints();

        assertEquals(0, manager.pendingHintCount());
        assertTrue(store.get("alpha").isPresent());
    }
}
