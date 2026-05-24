package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;

import java.util.concurrent.CompletableFuture;

public interface ReplicaClient {
    CompletableFuture<ReplicaWriteResult> write(NodeEndpoint node, String key, VersionedValue value);

    CompletableFuture<ReplicaReadResult> read(NodeEndpoint node, String key);
}
