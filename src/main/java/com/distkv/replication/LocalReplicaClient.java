package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.storage.KeyValueStore;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalReplicaClient implements ReplicaClient {
    private final Map<String, KeyValueStore> storesByNodeId = new ConcurrentHashMap<>();

    public void register(NodeEndpoint node, KeyValueStore store) {
        storesByNodeId.put(node.nodeId(), Objects.requireNonNull(store, "store"));
    }

    @Override
    public CompletableFuture<ReplicaWriteResult> write(NodeEndpoint node, String key, VersionedValue value) {
        return CompletableFuture.supplyAsync(() -> {
            KeyValueStore store = storesByNodeId.get(node.nodeId());
            if (store == null) {
                return ReplicaWriteResult.failed("node " + node.nodeId() + " is not reachable");
            }
            store.apply(key, value);
            return ReplicaWriteResult.ok();
        });
    }

    @Override
    public CompletableFuture<ReplicaReadResult> read(NodeEndpoint node, String key) {
        return CompletableFuture.supplyAsync(() -> {
            KeyValueStore store = storesByNodeId.get(node.nodeId());
            if (store == null) {
                return ReplicaReadResult.failed("node " + node.nodeId() + " is not reachable");
            }
            List<VersionedValue> values = store.getVersionsIncludingTombstone(key);
            return values.isEmpty() ? ReplicaReadResult.notFound() : ReplicaReadResult.found(values);
        });
    }
}
