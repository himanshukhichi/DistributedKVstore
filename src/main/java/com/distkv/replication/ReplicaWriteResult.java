package com.distkv.replication;

public record ReplicaWriteResult(boolean acknowledged, String message) {
    public static ReplicaWriteResult ok() {
        return new ReplicaWriteResult(true, "acknowledged");
    }

    public static ReplicaWriteResult failed(String message) {
        return new ReplicaWriteResult(false, message);
    }
}
