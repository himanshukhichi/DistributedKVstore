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

class WALManagerTest {
    @TempDir
    Path tempDir;

    @Test
    void replayRestoresLatestValueForEveryKey() throws IOException {
        Clock clock = Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC);
        WALManager walManager = new WALManager(tempDir.resolve("node-a.wal"));
        walManager.append("alpha", VersionedValue.put("one".getBytes(), "node-a", 1, clock));
        walManager.append("beta", VersionedValue.put("two".getBytes(), "node-a", 2, clock));
        walManager.append("alpha", VersionedValue.put("three".getBytes(), "node-a", 3, clock));

        Map<String, VersionedValue> restored = walManager.replay();

        assertEquals(2, restored.size());
        assertArrayEquals("three".getBytes(), restored.get("alpha").value());
        assertArrayEquals("two".getBytes(), restored.get("beta").value());
        assertFalse(restored.get("alpha").tombstone());
    }

    @Test
    void replayPreservesDeleteTombstone() throws IOException {
        Clock clock = Clock.fixed(Instant.parse("2026-05-24T12:00:00Z"), ZoneOffset.UTC);
        WALManager walManager = new WALManager(tempDir.resolve("node-a.wal"));
        walManager.append("alpha", VersionedValue.put("one".getBytes(), "node-a", 1, clock));
        walManager.append("alpha", VersionedValue.tombstone("node-a", 2, clock));

        Map<String, VersionedValue> restored = walManager.replay();

        assertTrue(restored.get("alpha").tombstone());
    }
}
