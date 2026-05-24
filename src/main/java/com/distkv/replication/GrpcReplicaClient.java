package com.distkv.replication;

import com.distkv.grpc.ProtoMappers;
import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.proto.ReplicaApplyRequest;
import com.distkv.proto.ReplicaApplyResponse;
import com.distkv.proto.ReplicaReadRequest;
import com.distkv.proto.ReplicaReadResponse;
import com.distkv.proto.ReplicaServiceGrpc;
import com.distkv.storage.KeyValueStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcReplicaClient implements ReplicaClient, AutoCloseable {
    private final NodeEndpoint localEndpoint;
    private final KeyValueStore localStore;
    private final Duration rpcTimeout;
    private final Map<String, ManagedChannel> channelsByNodeId = new ConcurrentHashMap<>();

    public GrpcReplicaClient(NodeEndpoint localEndpoint, KeyValueStore localStore, Duration rpcTimeout) {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.localStore = Objects.requireNonNull(localStore, "localStore");
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
    }

    @Override
    public CompletableFuture<ReplicaWriteResult> write(NodeEndpoint node, String key, VersionedValue value) {
        if (node.nodeId().equals(localEndpoint.nodeId())) {
            return CompletableFuture.supplyAsync(() -> {
                localStore.apply(key, value);
                return ReplicaWriteResult.ok();
            });
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                ReplicaApplyResponse response = blockingStub(node).apply(ReplicaApplyRequest.newBuilder()
                        .setKey(key)
                        .setVersion(ProtoMappers.toProto(value))
                        .build());
                return response.getSuccess()
                        ? ReplicaWriteResult.ok()
                        : ReplicaWriteResult.failed(response.getMessage());
            } catch (StatusRuntimeException exception) {
                return ReplicaWriteResult.failed(exception.getStatus().toString());
            }
        });
    }

    @Override
    public CompletableFuture<ReplicaReadResult> read(NodeEndpoint node, String key) {
        if (node.nodeId().equals(localEndpoint.nodeId())) {
            return CompletableFuture.supplyAsync(() -> {
                Optional<VersionedValue> value = localStore.getIncludingTombstone(key);
                return value.map(ReplicaReadResult::found).orElseGet(ReplicaReadResult::notFound);
            });
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                ReplicaReadResponse response = blockingStub(node).read(ReplicaReadRequest.newBuilder()
                        .setKey(key)
                        .build());
                if (!response.getResponded()) {
                    return ReplicaReadResult.failed(response.getMessage());
                }
                return response.getFound()
                        ? ReplicaReadResult.found(ProtoMappers.fromProto(response.getVersion()))
                        : ReplicaReadResult.notFound();
            } catch (StatusRuntimeException exception) {
                return ReplicaReadResult.failed(exception.getStatus().toString());
            }
        });
    }

    @Override
    public void close() {
        channelsByNodeId.values().forEach(channel -> {
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private ReplicaServiceGrpc.ReplicaServiceBlockingStub blockingStub(NodeEndpoint node) {
        ManagedChannel channel = channelsByNodeId.computeIfAbsent(node.nodeId(), ignored ->
                ManagedChannelBuilder.forAddress(node.host(), node.grpcPort())
                        .usePlaintext()
                        .build());
        return ReplicaServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
