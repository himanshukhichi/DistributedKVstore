package com.distkv.server;

import com.distkv.grpc.AdminServiceImpl;
import com.distkv.grpc.KVServiceImpl;
import com.distkv.grpc.ReplicaServiceImpl;
import com.distkv.membership.GrpcGossipPeerClient;
import com.distkv.membership.GossipService;
import com.distkv.membership.MembershipList;
import com.distkv.model.NodeEndpoint;
import com.distkv.observability.DistKvMetrics;
import com.distkv.replication.GrpcReplicaClient;
import com.distkv.replication.HintedHandoffManager;
import com.distkv.replication.QuorumCoordinator;
import com.distkv.repair.AntiEntropyService;
import com.distkv.repair.GrpcAntiEntropyPeerClient;
import com.distkv.routing.ConsistentHashRing;
import com.distkv.storage.InMemoryKeyValueStore;
import com.distkv.storage.WALManager;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.NodeInfo;
import com.distkv.proto.NodeJoinRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class DistKvServer {
    private DistKvServer() {
    }

    public static void main(String[] args) throws Exception {
        String nodeId = getenv("DISTKV_NODE_ID", "node-1");
        String host = getenv("DISTKV_HOST", "localhost");
        int port = Integer.parseInt(getenv("DISTKV_PORT", "50051"));
        int metricsPort = Integer.parseInt(getenv("DISTKV_METRICS_PORT", "9100"));
        int replicationFactor = Integer.parseInt(getenv("DISTKV_REPLICATION_FACTOR", "3"));
        int maxEntries = Integer.parseInt(getenv("DISTKV_MAX_ENTRIES", "-1"));
        long bloomExpectedInsertions = Long.parseLong(getenv("DISTKV_BLOOM_EXPECTED_INSERTIONS", "100000"));
        double bloomFalsePositiveRate = Double.parseDouble(getenv("DISTKV_BLOOM_FALSE_POSITIVE_RATE", "0.01"));
        long walCompactionWrites = Long.parseLong(getenv("DISTKV_WAL_COMPACTION_WRITES", "10000"));
        Path dataDir = Path.of(getenv("DISTKV_DATA_DIR", "data/" + nodeId));

        Clock clock = Clock.systemUTC();
        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, host, port);

        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode(localEndpoint);

        WALManager walManager = new WALManager(dataDir.resolve("wal.log"), walCompactionWrites);
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(
                clock,
                walManager,
                maxEntries,
                new com.distkv.storage.BloomFilter(bloomExpectedInsertions, bloomFalsePositiveRate));
        store.recoverFromWal();
        DistKvMetrics metrics = new DistKvMetrics();
        metrics.startHttpServer(metricsPort);

        MembershipList membershipList = new MembershipList(localEndpoint, clock);
        List<NodeEndpoint> peers = parsePeers(getenv("DISTKV_PEERS", ""));
        peers.forEach(peer -> {
            ring.addNode(peer);
            membershipList.addOrMarkAlive(peer);
        });

        GrpcReplicaClient replicaClient = new GrpcReplicaClient(localEndpoint, store, Duration.ofMillis(500));
        HintedHandoffManager hintedHandoffManager = new HintedHandoffManager(replicaClient, Duration.ofSeconds(2));
        hintedHandoffManager.start();

        QuorumCoordinator coordinator = new QuorumCoordinator(
                nodeId,
                ring,
                replicaClient,
                replicationFactor,
                Duration.ofMillis(750),
                clock,
                hintedHandoffManager);

        GrpcGossipPeerClient peerClient = new GrpcGossipPeerClient(Duration.ofMillis(500));
        GossipService gossipService = new GossipService(
                membershipList,
                peerClient,
                member -> ring.removeNode(member.endpoint().nodeId()),
                Duration.ofSeconds(1),
                GossipService.DEFAULT_FANOUT,
                GossipService.DEFAULT_SUSPECT_AFTER_CYCLES,
                GossipService.DEFAULT_DEAD_AFTER_CYCLES);
        gossipService.start();
        GrpcAntiEntropyPeerClient antiEntropyPeerClient = new GrpcAntiEntropyPeerClient(Duration.ofMillis(750));
        AntiEntropyService antiEntropyService = new AntiEntropyService(
                store,
                () -> ring.nodes().stream()
                        .filter(node -> !node.nodeId().equals(localEndpoint.nodeId()))
                        .toList(),
                antiEntropyPeerClient,
                Duration.ofSeconds(Long.parseLong(getenv("DISTKV_ANTI_ENTROPY_INTERVAL_SECONDS", "30"))));
        antiEntropyService.start();

        Server server = ServerBuilder.forPort(port)
                .addService(new KVServiceImpl(coordinator, store, metrics))
                .addService(new AdminServiceImpl(membershipList, ring))
                .addService(new ReplicaServiceImpl(store))
                .build()
                .start();
        announceJoin(localEndpoint, peers, clock);
        ScheduledExecutorService metricsReporter = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "distkv-metrics-reporter");
            thread.setDaemon(true);
            return thread;
        });
        metricsReporter.scheduleAtFixedRate(() -> {
            try {
                metrics.setNodeHealth(membershipList.snapshot());
                metrics.setWalSizeBytes(nodeId, walManager.sizeBytes());
                metrics.setPendingHints(nodeId, hintedHandoffManager.pendingHintCount());
                metrics.setReplicationLagMs(nodeId, hintedHandoffManager.oldestHintAgeMillis(System.currentTimeMillis()));
            } catch (Exception ignored) {
                // Metrics reporting should never interrupt request handling.
            }
        }, 0, 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            metricsReporter.shutdownNow();
            gossipService.close();
            peerClient.close();
            antiEntropyService.close();
            antiEntropyPeerClient.close();
            hintedHandoffManager.close();
            metrics.close();
            replicaClient.close();
            server.shutdown();
        }, "distkv-shutdown"));

        System.out.printf("DistKV node %s listening on %s:%d%n", nodeId, host, port);
        server.awaitTermination();
    }

    private static String getenv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static List<NodeEndpoint> parsePeers(String rawPeers) {
        if (rawPeers == null || rawPeers.isBlank()) {
            return List.of();
        }
        return Arrays.stream(rawPeers.split(","))
                .map(String::trim)
                .filter(peer -> !peer.isBlank())
                .map(peer -> {
                    String[] parts = peer.split(":");
                    if (parts.length != 3) {
                        throw new IllegalArgumentException("peer must use nodeId:host:port format: " + peer);
                    }
                    return new NodeEndpoint(parts[0], parts[1], Integer.parseInt(parts[2]));
                })
                .toList();
    }

    private static void announceJoin(NodeEndpoint localEndpoint, List<NodeEndpoint> peers, Clock clock) {
        NodeInfo localNode = NodeInfo.newBuilder()
                .setNodeId(localEndpoint.nodeId())
                .setHost(localEndpoint.host())
                .setGrpcPort(localEndpoint.grpcPort())
                .setHealthy(true)
                .setStatus("ALIVE")
                .setHeartbeat(0)
                .setLastSeenEpochMs(clock.millis())
                .build();
        NodeJoinRequest request = NodeJoinRequest.newBuilder()
                .setNode(localNode)
                .build();
        peers.forEach(peer -> {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.host(), peer.grpcPort())
                    .usePlaintext()
                    .build();
            try {
                AdminServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(500, TimeUnit.MILLISECONDS)
                        .nodeJoin(request);
            } catch (RuntimeException ignored) {
                // Static peers may not be running yet; gossip will catch them once they start.
            } finally {
                channel.shutdownNow();
            }
        });
    }
}
