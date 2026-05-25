package com.distkv.storage;

import com.distkv.model.ClockRelation;
import com.distkv.model.VersionedValue;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryKeyValueStore implements KeyValueStore {
    private static final int UNLIMITED_ENTRIES = -1;

    private final ConcurrentHashMap<String, List<VersionedValue>> values = new ConcurrentHashMap<>();
    private final LinkedHashMap<String, Boolean> accessOrder = new LinkedHashMap<>(16, 0.75f, true);
    private final Clock clock;
    private final WALManager walManager;
    private final int maxEntries;
    private final BloomFilter bloomFilter;

    public InMemoryKeyValueStore(Clock clock) {
        this(clock, null, UNLIMITED_ENTRIES, null);
    }

    public InMemoryKeyValueStore(Clock clock, WALManager walManager) {
        this(clock, walManager, UNLIMITED_ENTRIES, null);
    }

    public InMemoryKeyValueStore(Clock clock, WALManager walManager, int maxEntries, BloomFilter bloomFilter) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.walManager = walManager;
        if (maxEntries == 0 || maxEntries < UNLIMITED_ENTRIES) {
            throw new IllegalArgumentException("maxEntries must be positive or -1 for unlimited");
        }
        this.maxEntries = maxEntries;
        this.bloomFilter = bloomFilter;
    }

    public synchronized void recoverFromWal() throws IOException {
        if (walManager == null) {
            return;
        }
        values.clear();
        accessOrder.clear();
        walManager.restore().forEach((key, versions) -> {
            values.put(key, List.copyOf(versions));
            touch(key);
            versions.stream().filter(value -> !value.tombstone()).findAny().ifPresent(ignored -> addToBloom(key));
        });
    }

    @Override
    public synchronized VersionedValue put(String key, byte[] value, String nodeId, long counter) {
        VersionedValue versionedValue = new VersionedValue(
                value,
                clock.millis(),
                VersionedValue.mergeVectorClocks(getVersionsIncludingTombstone(key), nodeId, counter),
                false);
        apply(key, versionedValue);
        return versionedValue;
    }

    @Override
    public synchronized VersionedValue delete(String key, String nodeId, long counter) {
        VersionedValue tombstone = new VersionedValue(
                new byte[0],
                clock.millis(),
                VersionedValue.mergeVectorClocks(getVersionsIncludingTombstone(key), nodeId, counter),
                true);
        apply(key, tombstone);
        return tombstone;
    }

    @Override
    public synchronized void apply(String key, VersionedValue value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        appendToWal(key, value);
        values.compute(key, (ignored, current) -> mergeVersions(current, value));
        touch(key);
        addToBloom(key);
        evictIfNeeded();
        compactWalIfNeeded();
    }

    @Override
    public synchronized Optional<VersionedValue> get(String key) {
        if (isDefinitelyAbsent(key)) {
            return Optional.empty();
        }
        touch(key);
        return getIncludingTombstone(key).filter(value -> !value.tombstone());
    }

    @Override
    public synchronized Optional<VersionedValue> getIncludingTombstone(String key) {
        if (isDefinitelyAbsent(key)) {
            return Optional.empty();
        }
        touch(key);
        return Optional.ofNullable(VersionedValue.latestIncludingTombstone(values.getOrDefault(key, List.of())));
    }

    @Override
    public synchronized List<VersionedValue> getVersions(String key) {
        if (isDefinitelyAbsent(key)) {
            return List.of();
        }
        touch(key);
        return values.getOrDefault(key, List.of()).stream()
                .filter(value -> !value.tombstone())
                .toList();
    }

    @Override
    public synchronized List<VersionedValue> getVersionsIncludingTombstone(String key) {
        if (isDefinitelyAbsent(key)) {
            return List.of();
        }
        touch(key);
        return List.copyOf(values.getOrDefault(key, List.of()));
    }

    @Override
    public synchronized List<StoredEntry> scan(String startKeyInclusive, String endKeyExclusive) {
        String start = startKeyInclusive == null ? "" : startKeyInclusive;
        String end = endKeyExclusive == null ? "" : endKeyExclusive;
        return values.entrySet().stream()
                .filter(entry -> entry.getKey().compareTo(start) >= 0)
                .filter(entry -> end.isBlank() || entry.getKey().compareTo(end) < 0)
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new StoredEntry(entry.getKey(), VersionedValue.latest(entry.getValue())))
                .filter(entry -> entry.value() != null)
                .toList();
    }

    @Override
    public synchronized Map<String, VersionedValue> snapshot() {
        Map<String, VersionedValue> snapshot = new LinkedHashMap<>();
        values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    VersionedValue latest = VersionedValue.latest(entry.getValue());
                    if (latest != null) {
                        snapshot.put(entry.getKey(), latest);
                    }
                });
        return snapshot;
    }

    @Override
    public synchronized Map<String, List<VersionedValue>> snapshotVersions() {
        return values.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(LinkedHashMap::new,
                        (map, entry) -> map.put(entry.getKey(), List.copyOf(entry.getValue())),
                        LinkedHashMap::putAll);
    }

    public synchronized List<String> sortedKeys() {
        return values.keySet().stream().sorted(Comparator.naturalOrder()).toList();
    }

    public synchronized int size() {
        return values.size();
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

    private void compactWalIfNeeded() {
        if (walManager == null || !walManager.shouldCompact()) {
            return;
        }
        try {
            walManager.compact(snapshotVersions());
        } catch (IOException exception) {
            throw new IllegalStateException("failed to compact WAL", exception);
        }
    }

    private List<VersionedValue> mergeVersions(List<VersionedValue> current, VersionedValue incoming) {
        if (current == null || current.isEmpty()) {
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
                VersionedValue winner = incoming.isNewerThan(existing) ? incoming : existing;
                merged.add(winner);
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

    private boolean isDefinitelyAbsent(String key) {
        return bloomFilter != null && !bloomFilter.mightContain(key);
    }

    private void addToBloom(String key) {
        if (bloomFilter != null) {
            bloomFilter.add(key);
        }
    }

    private void touch(String key) {
        if (maxEntries == UNLIMITED_ENTRIES || key == null || key.isBlank()) {
            return;
        }
        accessOrder.put(key, Boolean.TRUE);
    }

    private void evictIfNeeded() {
        if (maxEntries == UNLIMITED_ENTRIES) {
            return;
        }
        while (values.size() > maxEntries && !accessOrder.isEmpty()) {
            String eldestKey = accessOrder.keySet().iterator().next();
            accessOrder.remove(eldestKey);
            values.remove(eldestKey);
        }
    }
}
