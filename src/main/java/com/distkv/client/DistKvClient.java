package com.distkv.client;

import com.distkv.model.NodeEndpoint;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.ClusterStatusRequest;
import com.distkv.proto.ConsistencyLevel;
import com.distkv.proto.DeleteRequest;
import com.distkv.proto.DeleteResponse;
import com.distkv.proto.Entry;
import com.distkv.proto.GetRequest;
import com.distkv.proto.GetResponse;
import com.distkv.proto.KVServiceGrpc;
import com.distkv.proto.NodeInfo;
import com.distkv.proto.PutRequest;
import com.distkv.proto.PutResponse;
import com.distkv.proto.ScanRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public final class DistKvClient implements AutoCloseable {
    private final Map<String, NodeEndpoint> nodesById;
    private final Map<String, Boolean> healthyById;
    private final Map<String, ManagedChannel> channelsById = new ConcurrentHashMap<>();
    private final ConsistencyLevel defaultConsistency;
    private final int maxRetries;
    private final Duration initialBackoff;
    private final Duration rpcTimeout;
    private final Random random = new Random();

    private DistKvClient(Builder builder) {
        this.nodesById = new ConcurrentHashMap<>(builder.nodesById);
        this.healthyById = new ConcurrentHashMap<>();
        this.nodesById.keySet().forEach(nodeId -> healthyById.put(nodeId, true));
        this.defaultConsistency = builder.defaultConsistency;
        this.maxRetries = builder.maxRetries;
        this.initialBackoff = builder.initialBackoff;
        this.rpcTimeout = builder.rpcTimeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    public PutResponse put(String key, byte[] value) {
        return put(key, value, defaultConsistency);
    }

    public PutResponse put(String key, byte[] value, ConsistencyLevel consistency) {
        Objects.requireNonNull(value, "value");
        PutRequest request = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .setConsistency(consistency)
                .build();
        return executeWithRetry(node -> kvStub(node).put(request));
    }

    public GetResponse get(String key) {
        return get(key, defaultConsistency);
    }

    public GetResponse get(String key, ConsistencyLevel consistency) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(key)
                .setConsistency(consistency)
                .build();
        return executeWithRetry(node -> kvStub(node).get(request));
    }

    public DeleteResponse delete(String key) {
        return delete(key, defaultConsistency);
    }

    public DeleteResponse delete(String key, ConsistencyLevel consistency) {
        DeleteRequest request = DeleteRequest.newBuilder()
                .setKey(key)
                .setConsistency(consistency)
                .build();
        return executeWithRetry(node -> kvStub(node).delete(request));
    }

    public List<Entry> scan(String startKeyInclusive, String endKeyExclusive) {
        Iterator<Entry> iterator = scanStreaming(startKeyInclusive, endKeyExclusive);
        List<Entry> entries = new ArrayList<>();
        iterator.forEachRemaining(entries::add);
        return entries;
    }

    public Iterator<Entry> scanStreaming(String startKeyInclusive, String endKeyExclusive) {
        ScanRequest request = ScanRequest.newBuilder()
                .setStartKey(startKeyInclusive == null ? "" : startKeyInclusive)
                .setEndKey(endKeyExclusive == null ? "" : endKeyExclusive)
                .setConsistency(defaultConsistency)
                .build();
        return executeWithRetry(node -> kvStub(node).scan(request));
    }

    public void refreshClusterStatus() {
        for (NodeEndpoint node : new ArrayList<>(nodesById.values())) {
            try {
                adminStub(node).clusterStatus(ClusterStatusRequest.newBuilder().build())
                        .getNodesList()
                        .forEach(this::upsertNode);
                healthyById.put(node.nodeId(), true);
                return;
            } catch (StatusRuntimeException exception) {
                healthyById.put(node.nodeId(), false);
            }
        }
    }

    @Override
    public void close() {
        channelsById.values().forEach(channel -> {
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private <T> T executeWithRetry(Function<NodeEndpoint, T> operation) {
        RuntimeException lastFailure = null;
        long backoffMillis = initialBackoff.toMillis();
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            NodeEndpoint node = pickHealthyNode();
            try {
                T result = operation.apply(node);
                healthyById.put(node.nodeId(), true);
                return result;
            } catch (StatusRuntimeException exception) {
                healthyById.put(node.nodeId(), false);
                lastFailure = exception;
                refreshClusterStatus();
                sleep(backoffMillis);
                backoffMillis *= 2;
            }
        }
        throw lastFailure == null ? new IllegalStateException("request failed without an exception") : lastFailure;
    }

    private NodeEndpoint pickHealthyNode() {
        List<NodeEndpoint> healthyNodes = nodesById.values().stream()
                .filter(node -> healthyById.getOrDefault(node.nodeId(), true))
                .toList();
        if (healthyNodes.isEmpty()) {
            refreshClusterStatus();
            healthyNodes = nodesById.values().stream()
                    .filter(node -> healthyById.getOrDefault(node.nodeId(), true))
                    .toList();
        }
        if (healthyNodes.isEmpty()) {
            throw new IllegalStateException("no healthy DistKV nodes available");
        }
        return healthyNodes.get(random.nextInt(healthyNodes.size()));
    }

    private KVServiceGrpc.KVServiceBlockingStub kvStub(NodeEndpoint node) {
        return KVServiceGrpc.newBlockingStub(channel(node))
                .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private AdminServiceGrpc.AdminServiceBlockingStub adminStub(NodeEndpoint node) {
        return AdminServiceGrpc.newBlockingStub(channel(node))
                .withDeadlineAfter(rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private ManagedChannel channel(NodeEndpoint node) {
        return channelsById.computeIfAbsent(node.nodeId(), ignored ->
                ManagedChannelBuilder.forAddress(node.host(), node.grpcPort())
                        .usePlaintext()
                        .build());
    }

    private void upsertNode(NodeInfo nodeInfo) {
        if (nodeInfo.getNodeId().isBlank()) {
            return;
        }
        NodeEndpoint endpoint = new NodeEndpoint(nodeInfo.getNodeId(), nodeInfo.getHost(), nodeInfo.getGrpcPort());
        nodesById.put(endpoint.nodeId(), endpoint);
        healthyById.put(endpoint.nodeId(), nodeInfo.getHealthy());
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {
        private final Map<String, NodeEndpoint> nodesById = new LinkedHashMap<>();
        private ConsistencyLevel defaultConsistency = ConsistencyLevel.QUORUM;
        private int maxRetries = 3;
        private Duration initialBackoff = Duration.ofMillis(50);
        private Duration rpcTimeout = Duration.ofSeconds(1);

        private Builder() {
        }

        public Builder addNode(String nodeId, String host, int grpcPort) {
            NodeEndpoint endpoint = new NodeEndpoint(nodeId, host, grpcPort);
            nodesById.put(endpoint.nodeId(), endpoint);
            return this;
        }

        public Builder addNode(String host, int grpcPort) {
            return addNode(host + ":" + grpcPort, host, grpcPort);
        }

        public Builder defaultConsistency(ConsistencyLevel defaultConsistency) {
            this.defaultConsistency = Objects.requireNonNull(defaultConsistency, "defaultConsistency");
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must be zero or greater");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder initialBackoff(Duration initialBackoff) {
            this.initialBackoff = Objects.requireNonNull(initialBackoff, "initialBackoff");
            return this;
        }

        public Builder rpcTimeout(Duration rpcTimeout) {
            this.rpcTimeout = Objects.requireNonNull(rpcTimeout, "rpcTimeout");
            return this;
        }

        public DistKvClient build() {
            if (nodesById.isEmpty()) {
                throw new IllegalStateException("at least one DistKV node is required");
            }
            return new DistKvClient(this);
        }
    }
}
