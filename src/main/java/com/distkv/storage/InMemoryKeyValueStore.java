package com.distkv.storage;

import com.distkv.model.VersionedValue;

import java.io.IOException;
import java.time.Clock;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryKeyValueStore implements KeyValueStore {
    private final ConcurrentHashMap<String, VersionedValue> values = new ConcurrentHashMap<>();
    private final Clock clock;
    private final WALManager walManager;

    public InMemoryKeyValueStore(Clock clock) {
        this(clock, null);
    }

    public InMemoryKeyValueStore(Clock clock, WALManager walManager) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.walManager = walManager;
    }

    public void recoverFromWal() throws IOException {
        if (walManager == null) {
            return;
        }
        values.clear();
        values.putAll(walManager.replay());
    }

    @Override
    public VersionedValue put(String key, byte[] value, String nodeId, long counter) {
        VersionedValue versionedValue = VersionedValue.put(value, nodeId, counter, clock);
        apply(key, versionedValue);
        return versionedValue;
    }

    @Override
    public VersionedValue delete(String key, String nodeId, long counter) {
        VersionedValue tombstone = VersionedValue.tombstone(nodeId, counter, clock);
        apply(key, tombstone);
        return tombstone;
    }

    @Override
    public void apply(String key, VersionedValue value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        appendToWal(key, value);
        values.compute(key, (ignored, current) -> value.isNewerThan(current) ? value : current);
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        return getIncludingTombstone(key).filter(value -> !value.tombstone());
    }

    @Override
    public Optional<VersionedValue> getIncludingTombstone(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public List<StoredEntry> scan(String startKeyInclusive, String endKeyExclusive) {
        String start = startKeyInclusive == null ? "" : startKeyInclusive;
        String end = endKeyExclusive == null ? "" : endKeyExclusive;
        return values.entrySet().stream()
                .filter(entry -> !entry.getValue().tombstone())
                .filter(entry -> entry.getKey().compareTo(start) >= 0)
                .filter(entry -> end.isBlank() || entry.getKey().compareTo(end) < 0)
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new StoredEntry(entry.getKey(), entry.getValue()))
                .toList();
    }

    @Override
    public Map<String, VersionedValue> snapshot() {
        return values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(LinkedHashMap::new,
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                        LinkedHashMap::putAll);
    }

    public List<String> sortedKeys() {
        return values.keySet().stream().sorted(Comparator.naturalOrder()).toList();
    }

    private void appendToWal(String key, VersionedValue value) {
        if (walManager == null) {
            return;
        }
        try {
            walManager.append(key, value);
        } catch (IOException exception) {
            throw new IllegalStateException("failed to append write to WAL", exception);
        }
    }
}
