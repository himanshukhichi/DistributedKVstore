package com.distkv.server;

import com.distkv.grpc.AdminServiceImpl;
import com.distkv.grpc.KVServiceImpl;
import com.distkv.grpc.ReplicaServiceImpl;
import com.distkv.membership.GossipPeerClient;
import com.distkv.membership.GossipService;
import com.distkv.membership.MembershipList;
import com.distkv.model.NodeEndpoint;
import com.distkv.replication.GrpcReplicaClient;
import com.distkv.replication.QuorumCoordinator;
import com.distkv.routing.ConsistentHashRing;
import com.distkv.storage.InMemoryKeyValueStore;
import com.distkv.storage.WALManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public final class DistKvServer {
    private DistKvServer() {
    }

    public static void main(String[] args) throws Exception {
        String nodeId = getenv("DISTKV_NODE_ID", "node-1");
        String host = getenv("DISTKV_HOST", "localhost");
        int port = Integer.parseInt(getenv("DISTKV_PORT", "50051"));
        int replicationFactor = Integer.parseInt(getenv("DISTKV_REPLICATION_FACTOR", "3"));
        Path dataDir = Path.of(getenv("DISTKV_DATA_DIR", "data/" + nodeId));

        Clock clock = Clock.systemUTC();
        NodeEndpoint localEndpoint = new NodeEndpoint(nodeId, host, port);

        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode(localEndpoint);

        WALManager walManager = new WALManager(dataDir.resolve("wal.log"));
        InMemoryKeyValueStore store = new InMemoryKeyValueStore(clock, walManager);
        store.recoverFromWal();

        MembershipList membershipList = new MembershipList(localEndpoint, clock);
        parsePeers(getenv("DISTKV_PEERS", "")).forEach(peer -> {
            ring.addNode(peer);
            membershipList.addOrMarkAlive(peer);
        });

        GrpcReplicaClient replicaClient = new GrpcReplicaClient(localEndpoint, store, Duration.ofMillis(500));

        QuorumCoordinator coordinator = new QuorumCoordinator(
                nodeId,
                ring,
                replicaClient,
                replicationFactor,
                Duration.ofMillis(750),
                clock);

        GossipPeerClient peerClient = (peer, localMembership) -> List.of();
        GossipService gossipService = new GossipService(membershipList, peerClient);
        gossipService.start();

        Server server = ServerBuilder.forPort(port)
                .addService(new KVServiceImpl(coordinator, store))
                .addService(new AdminServiceImpl(membershipList, ring))
                .addService(new ReplicaServiceImpl(store))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            gossipService.close();
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
}
