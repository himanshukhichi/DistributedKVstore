package com.distkv.repair;

import com.distkv.grpc.ProtoMappers;
import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.proto.FetchVersionsRequest;
import com.distkv.proto.MerkleRequest;
import com.distkv.proto.MerkleResponse;
import com.distkv.proto.ReplicaApplyVersionsRequest;
import com.distkv.proto.ReplicaServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcAntiEntropyPeerClient implements AntiEntropyPeerClient, AutoCloseable {
    private final Duration rpcTimeout;
    private final Map<String, ManagedChannel> channelsByNodeId = new ConcurrentHashMap<>();

    public GrpcAntiEntropyPeerClient(Duration rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
    }

    @Override
    public MerkleTree fetchMerkle(NodeEndpoint peer) {
        MerkleResponse response = stub(peer).merkle(MerkleRequest.newBuilder().build());
        return new MerkleTree(response.getRootHash(), response.getLeafHashesMap());
    }

    @Override
    public List<VersionedValue> fetchVersions(NodeEndpoint peer, String key) {
        return stub(peer).fetchVersions(FetchVersionsRequest.newBuilder().setKey(key).build())
                .getVersionsList()
                .stream()
                .map(ProtoMappers::fromProto)
                .toList();
    }

    @Override
    public void applyVersions(NodeEndpoint peer, String key, List<VersionedValue> versions) {
        stub(peer).applyVersions(ReplicaApplyVersionsRequest.newBuilder()
                .setKey(key)
                .addAllVersions(versions.stream().map(ProtoMappers::toProto).toList())
                .build());
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

    private ReplicaServiceGrpc.ReplicaServiceBlockingStub stub(NodeEndpoint peer) {
        ManagedChannel channel = channelsByNodeId.computeIfAbsent(peer.nodeId(), ignored ->
                ManagedChannelBuilder.forAddress(peer.host(), peer.grpcPort())
                        .usePlaintext()
                        .build());
        return ReplicaServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
