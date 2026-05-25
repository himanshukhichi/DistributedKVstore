package com.distkv.client;

import com.distkv.model.NodeEndpoint;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.ClusterStatusRequest;
import com.distkv.proto.ConsistencyLevel;
import com.distkv.proto.Entry;
import com.distkv.proto.GetResponse;
import com.distkv.proto.NodeInfo;
import com.distkv.proto.PutResponse;
import com.distkv.proto.ReplicaReadRequest;
import com.distkv.proto.ReplicaReadResponse;
import com.distkv.proto.ReplicaServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class DistKvDemoClient {
    private static final List<NodeEndpoint> DEFAULT_NODES = List.of(
            new NodeEndpoint("node-1", "localhost", 50051),
            new NodeEndpoint("node-2", "localhost", 50052),
            new NodeEndpoint("node-3", "localhost", 50053));

    private DistKvDemoClient() {
    }

    public static void main(String[] args) throws Exception {
        String command = args.length == 0 ? "smoke" : args[0];
        try (DistKvClient client = baseClient()) {
            switch (command) {
                case "cluster" -> printCluster();
                case "smoke" -> runSmoke(client);
                case "put-range" -> putRange(client, args);
                case "get" -> get(client, args);
                case "scan" -> scan(client, args);
                case "replica-read" -> replicaRead(args);
                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(2);
                }
            }
        }
    }

    private static DistKvClient baseClient() {
        DistKvClient.Builder builder = DistKvClient.builder()
                .defaultConsistency(ConsistencyLevel.QUORUM)
                .maxRetries(5)
                .initialBackoff(Duration.ofMillis(75))
                .rpcTimeout(Duration.ofSeconds(2));
        DEFAULT_NODES.forEach(node -> builder.addNode(node.nodeId(), node.host(), node.grpcPort()));
        return builder.build();
    }

    private static void runSmoke(DistKvClient client) {
        System.out.println("== Cluster status ==");
        printCluster();

        System.out.println("== QUORUM writes ==");
        put(client, "demo-alpha", "alpha-value");
        put(client, "demo-beta", "beta-value");
        put(client, "demo-gamma", "gamma-value");

        System.out.println("== QUORUM read ==");
        printGet("demo-beta", client.get("demo-beta", ConsistencyLevel.QUORUM));

        System.out.println("== Streaming scan demo-a..demo-z ==");
        client.scan("demo-a", "demo-z").forEach(DistKvDemoClient::printEntry);
    }

    private static void putRange(DistKvClient client, String[] args) {
        if (args.length != 3) {
            printUsage();
            System.exit(2);
        }
        String prefix = args[1];
        int count = Integer.parseInt(args[2]);
        for (int i = 1; i <= count; i++) {
            String key = prefix + "-" + i;
            put(client, key, "value-" + key);
        }
        System.out.printf("Wrote %d keys with prefix %s using QUORUM%n", count, prefix);
    }

    private static void get(DistKvClient client, String[] args) {
        if (args.length != 2) {
            printUsage();
            System.exit(2);
        }
        printGet(args[1], client.get(args[1], ConsistencyLevel.QUORUM));
    }

    private static void scan(DistKvClient client, String[] args) {
        if (args.length != 3) {
            printUsage();
            System.exit(2);
        }
        client.scan(args[1], args[2]).forEach(DistKvDemoClient::printEntry);
    }

    private static void replicaRead(String[] args) {
        if (args.length != 4) {
            printUsage();
            System.exit(2);
        }
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String key = args[3];
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            ReplicaReadResponse response = ReplicaServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(2, TimeUnit.SECONDS)
                    .read(ReplicaReadRequest.newBuilder().setKey(key).build());
            System.out.printf("REPLICA_READ %s:%d key=%s responded=%s found=%s versions=%d message=%s%n",
                    host,
                    port,
                    key,
                    response.getResponded(),
                    response.getFound(),
                    response.getVersionsCount(),
                    response.getMessage());
            response.getVersionsList().forEach(version ->
                    System.out.printf("  value=%s clock=%s tombstone=%s%n",
                            version.getValue().toString(StandardCharsets.UTF_8),
                            version.getVectorClockMap(),
                            version.getTombstone()));
        } finally {
            channel.shutdownNow();
        }
    }

    private static void put(DistKvClient client, String key, String value) {
        PutResponse response = client.put(key, value.getBytes(StandardCharsets.UTF_8), ConsistencyLevel.QUORUM);
        System.out.printf("PUT %-18s success=%s acks=%d/%d clock=%s%n",
                key,
                response.getSuccess(),
                response.getAcksReceived(),
                response.getAcksRequired(),
                response.hasVersion() ? response.getVersion().getVectorClockMap() : "{}");
    }

    private static void printGet(String key, GetResponse response) {
        System.out.printf("GET %-18s found=%s replicas_contacted=%d versions=%d message=%s%n",
                key,
                response.getFound(),
                response.getReplicasContacted(),
                response.getVersionsCount(),
                response.getMessage());
        response.getVersionsList().forEach(version ->
                System.out.printf("  value=%s clock=%s tombstone=%s%n",
                        version.getValue().toString(StandardCharsets.UTF_8),
                        version.getVectorClockMap(),
                        version.getTombstone()));
    }

    private static void printEntry(Entry entry) {
        System.out.printf("SCAN %-18s value=%s clock=%s%n",
                entry.getKey(),
                entry.getVersion().getValue().toString(StandardCharsets.UTF_8),
                entry.getVersion().getVectorClockMap());
    }

    private static void printCluster() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051)
                .usePlaintext()
                .build();
        try {
            AdminServiceGrpc.AdminServiceBlockingStub stub = AdminServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(2, TimeUnit.SECONDS);
            stub.clusterStatus(ClusterStatusRequest.newBuilder().build())
                    .getNodesList()
                    .forEach(DistKvDemoClient::printNode);
        } finally {
            channel.shutdownNow();
        }
    }

    private static void printNode(NodeInfo node) {
        System.out.printf("NODE %-8s %s:%d healthy=%s status=%s heartbeat=%d%n",
                node.getNodeId(),
                node.getHost(),
                node.getGrpcPort(),
                node.getHealthy(),
                node.getStatus(),
                node.getHeartbeat());
    }

    private static void printUsage() {
        System.err.println("Usage:");
        System.err.println("  DistKvDemoClient smoke");
        System.err.println("  DistKvDemoClient cluster");
        System.err.println("  DistKvDemoClient put-range <prefix> <count>");
        System.err.println("  DistKvDemoClient get <key>");
        System.err.println("  DistKvDemoClient scan <startKey> <endKey>");
        System.err.println("  DistKvDemoClient replica-read <host> <port> <key>");
    }
}
