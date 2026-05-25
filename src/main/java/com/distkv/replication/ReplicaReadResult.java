package com.distkv.replication;

import com.distkv.model.VersionedValue;

import java.util.List;
import java.util.Optional;

public record ReplicaReadResult(boolean responded, List<VersionedValue> versions, String message) {
    public static ReplicaReadResult found(VersionedValue value) {
        return new ReplicaReadResult(true, List.of(value), "found");
    }

    public static ReplicaReadResult found(List<VersionedValue> values) {
        return new ReplicaReadResult(true, List.copyOf(values), "found");
    }

    public static ReplicaReadResult notFound() {
        return new ReplicaReadResult(true, List.of(), "not found");
    }

    public static ReplicaReadResult failed(String message) {
        return new ReplicaReadResult(false, List.of(), message);
    }

    public Optional<VersionedValue> value() {
        return Optional.ofNullable(VersionedValue.latestIncludingTombstone(versions));
    }
}
