package com.distkv.replication;

import com.distkv.model.VersionedValue;

import java.util.Optional;

public record ReplicaReadResult(boolean responded, Optional<VersionedValue> value, String message) {
    public static ReplicaReadResult found(VersionedValue value) {
        return new ReplicaReadResult(true, Optional.of(value), "found");
    }

    public static ReplicaReadResult notFound() {
        return new ReplicaReadResult(true, Optional.empty(), "not found");
    }

    public static ReplicaReadResult failed(String message) {
        return new ReplicaReadResult(false, Optional.empty(), message);
    }
}
