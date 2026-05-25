package com.distkv.storage;

import com.distkv.model.VersionedValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryKeyValueStoreTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC);

    @TempDir
    Path tempDir;

    @Test
    void storesConcurrentVectorClockVersionsAsSiblingsUntilResolved() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock);
        store.apply("alpha", new VersionedValue("left".getBytes(), 100, Map.of("node-a", 1L), false));
        store.apply("alpha", new VersionedValue("right".getBytes(), 101, Map.of("node-b", 1L), false));

        assertEquals(2, store.getVersions("alpha").size());

        store.apply("alpha", new VersionedValue(
                "resolved".getBytes(),
                102,
                Map.of("node-a", 1L, "node-b", 1L, "node-c", 1L),
                false));

        assertEquals(1, store.getVersions("alpha").size());
        assertArrayEquals("resolved".getBytes(), store.get("alpha").orElseThrow().value());
    }

    @Test
    void evictsLeastRecentlyUsedKeyWhenMaxEntriesIsReached() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock, null, 2, null);
        store.put("alpha", "one".getBytes(), "node-a", 1);
        store.put("beta", "two".getBytes(), "node-a", 2);
        assertTrue(store.get("alpha").isPresent());

        store.put("gamma", "three".getBytes(), "node-a", 3);

        assertTrue(store.get("alpha").isPresent());
        assertFalse(store.get("beta").isPresent());
        assertTrue(store.get("gamma").isPresent());
    }

    @Test
    void evictsLeastRecentlyUsedKeyWhenMaxMemoryBytesIsReached() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock, null, -1, 160, null);
        store.put("alpha", "one".getBytes(), "node-a", 1);
        store.put("beta", "two".getBytes(), "node-a", 2);

        assertFalse(store.get("alpha").isPresent());
        assertTrue(store.get("beta").isPresent());
    }

    @Test
    void compactsWalToSnapshotAndRecoversFromSnapshot() throws IOException {
        WALManager walManager = new WALManager(tempDir.resolve("node-a.wal"), 2);
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock, walManager);
        store.put("alpha", "one".getBytes(), "node-a", 1);
        store.put("beta", "two".getBytes(), "node-a", 2);

        assertEquals(0, walManager.sizeBytes());
        assertTrue(java.nio.file.Files.exists(walManager.snapshotPath()));

        InMemoryKeyValueStore recovered = new InMemoryKeyValueStore(clock, walManager);
        recovered.recoverFromWal();

        assertArrayEquals("one".getBytes(), recovered.get("alpha").orElseThrow().value());
        assertArrayEquals("two".getBytes(), recovered.get("beta").orElseThrow().value());
    }
}
