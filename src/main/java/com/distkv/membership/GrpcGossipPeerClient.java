package com.distkv.membership;

import com.distkv.model.NodeEndpoint;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.ClusterStatusRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class GrpcGossipPeerClient implements GossipPeerClient, AutoCloseable {
    private final Duration rpcTimeout;
    private final Map<String, ManagedChannel> channelsByNodeId = new ConcurrentHashMap<>();

    public GrpcGossipPeerClient(Duration rpcTimeout) {
        this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
    }

    @Override
    public List<MemberInfo> exchange(NodeEndpoint peer, List<MemberInfo> localMembership) {
        return AdminServiceGrpc.newBlockingStub(channel(peer))
                .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .clusterStatus(ClusterStatusRequest.newBuilder().build())
                .getNodesList()
                .stream()
                .map(node -> new MemberInfo(
                        new NodeEndpoint(node.getNodeId(), node.getHost(), node.getGrpcPort()),
                        node.getHeartbeat(),
                        node.getLastSeenEpochMs(),
                        MemberStatus.valueOf(node.getStatus())))
                .toList();
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

    private ManagedChannel channel(NodeEndpoint peer) {
        return channelsByNodeId.computeIfAbsent(peer.nodeId(), ignored ->
                ManagedChannelBuilder.forAddress(peer.host(), peer.grpcPort())
                        .usePlaintext()
                        .build());
    }
}
