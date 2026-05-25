package com.distkv.model;

import java.time.Clock;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class VersionedValue {
    private final byte[] value;
    private final long timestampEpochMs;
    private final Map<String, Long> vectorClock;
    private final boolean tombstone;

    public VersionedValue(byte[] value, long timestampEpochMs, Map<String, Long> vectorClock, boolean tombstone) {
        this.value = value == null ? new byte[0] : Arrays.copyOf(value, value.length);
        this.timestampEpochMs = timestampEpochMs;
        this.vectorClock = Map.copyOf(Objects.requireNonNull(vectorClock, "vectorClock"));
        this.tombstone = tombstone;
    }

    public static VersionedValue put(byte[] value, String nodeId, long counter, Clock clock) {
        return new VersionedValue(value, clock.millis(), Map.of(nodeId, counter), false);
    }

    public static VersionedValue tombstone(String nodeId, long counter, Clock clock) {
        return new VersionedValue(new byte[0], clock.millis(), Map.of(nodeId, counter), true);
    }

    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    public long timestampEpochMs() {
        return timestampEpochMs;
    }

    public Map<String, Long> vectorClock() {
        return new LinkedHashMap<>(vectorClock);
    }

    public boolean tombstone() {
        return tombstone;
    }

    public boolean isNewerThan(VersionedValue other) {
        if (other == null) {
            return true;
        }
        ClockRelation relation = compareVectorClock(other);
        if (relation == ClockRelation.AFTER) {
            return true;
        }
        if (relation == ClockRelation.BEFORE) {
            return false;
        }
        return timestampEpochMs > other.timestampEpochMs;
    }

    public ClockRelation compareVectorClock(VersionedValue other) {
        Objects.requireNonNull(other, "other");
        boolean greater = false;
        boolean less = false;
        for (String nodeId : unionKeys(other)) {
            long left = vectorClock.getOrDefault(nodeId, 0L);
            long right = other.vectorClock.getOrDefault(nodeId, 0L);
            if (left > right) {
                greater = true;
            } else if (left < right) {
                less = true;
            }
        }
        if (greater && !less) {
            return ClockRelation.AFTER;
        }
        if (less && !greater) {
            return ClockRelation.BEFORE;
        }
        if (!greater) {
            return ClockRelation.EQUAL;
        }
        return ClockRelation.CONCURRENT;
    }

    public static VersionedValue latest(List<VersionedValue> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        VersionedValue latest = null;
        for (VersionedValue value : values) {
            if (!value.tombstone() && value.isNewerThan(latest)) {
                latest = value;
            }
        }
        return latest;
    }

    public static VersionedValue latestIncludingTombstone(List<VersionedValue> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        VersionedValue latest = null;
        for (VersionedValue value : values) {
            if (value.isNewerThan(latest)) {
                latest = value;
            }
        }
        return latest;
    }

    public static List<VersionedValue> visible(List<VersionedValue> values) {
        return values.stream()
                .filter(value -> !value.tombstone())
                .toList();
    }

    public static Map<String, Long> mergeVectorClocks(List<VersionedValue> values, String nodeId, long nextCounter) {
        Map<String, Long> merged = new LinkedHashMap<>();
        for (VersionedValue value : values) {
            value.vectorClock.forEach((clockNodeId, counter) ->
                    merged.merge(clockNodeId, counter, Math::max));
        }
        merged.put(nodeId, Math.max(merged.getOrDefault(nodeId, 0L), nextCounter));
        return merged;
    }

    private Iterable<String> unionKeys(VersionedValue other) {
        Map<String, Boolean> keys = new LinkedHashMap<>();
        vectorClock.keySet().forEach(key -> keys.put(key, true));
        other.vectorClock.keySet().forEach(key -> keys.put(key, true));
        return keys.keySet();
    }
}
