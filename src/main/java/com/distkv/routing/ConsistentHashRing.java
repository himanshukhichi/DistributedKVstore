package com.distkv.routing;

import com.distkv.model.NodeEndpoint;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

public final class ConsistentHashRing {
    public static final int DEFAULT_VIRTUAL_NODES = 150;

    private final int virtualNodes;
    private final NavigableMap<BigInteger, NodeEndpoint> ring = new TreeMap<>();
    private final Map<String, NodeEndpoint> physicalNodes = new LinkedHashMap<>();
    private final Map<String, List<BigInteger>> tokensByNode = new LinkedHashMap<>();

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes < 1) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        this.virtualNodes = virtualNodes;
    }

    public synchronized void addNode(NodeEndpoint node) {
        Objects.requireNonNull(node, "node");
        removeNode(node.nodeId());
        physicalNodes.put(node.nodeId(), node);
        List<BigInteger> tokens = new ArrayList<>(virtualNodes);
        for (int index = 0; index < virtualNodes; index++) {
            BigInteger token = hash(node.nodeId() + "#" + index);
            ring.put(token, node);
            tokens.add(token);
        }
        tokensByNode.put(node.nodeId(), tokens);
    }

    public synchronized boolean removeNode(String nodeId) {
        List<BigInteger> tokens = tokensByNode.remove(nodeId);
        NodeEndpoint removed = physicalNodes.remove(nodeId);
        if (tokens != null) {
            tokens.forEach(ring::remove);
        }
        return removed != null;
    }

    public synchronized Optional<NodeEndpoint> getCoordinator(String key) {
        return getPreferenceList(key, 1).stream().findFirst();
    }

    public synchronized List<NodeEndpoint> getPreferenceList(String key, int replicaCount) {
        Objects.requireNonNull(key, "key");
        if (replicaCount < 1) {
            throw new IllegalArgumentException("replicaCount must be positive");
        }
        if (ring.isEmpty()) {
            return List.of();
        }

        BigInteger keyHash = hash(key);
        List<NodeEndpoint> result = new ArrayList<>(Math.min(replicaCount, physicalNodes.size()));
        LinkedHashSet<String> seenPhysicalNodes = new LinkedHashSet<>();
        collectDistinctNodes(ring.tailMap(keyHash, true).values(), replicaCount, result, seenPhysicalNodes);
        if (result.size() < replicaCount) {
            collectDistinctNodes(ring.headMap(keyHash, false).values(), replicaCount, result, seenPhysicalNodes);
        }
        return List.copyOf(result);
    }

    public synchronized List<NodeEndpoint> nodes() {
        return List.copyOf(physicalNodes.values());
    }

    public synchronized int size() {
        return physicalNodes.size();
    }

    public int virtualNodes() {
        return virtualNodes;
    }

    public static BigInteger hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            return new BigInteger(1, digest.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("MD5 digest is not available", exception);
        }
    }

    private void collectDistinctNodes(Collection<NodeEndpoint> candidates, int replicaCount,
                                      List<NodeEndpoint> result, LinkedHashSet<String> seenPhysicalNodes) {
        for (NodeEndpoint candidate : candidates) {
            if (seenPhysicalNodes.add(candidate.nodeId())) {
                result.add(candidate);
                if (result.size() == Math.min(replicaCount, physicalNodes.size())) {
                    return;
                }
            }
        }
    }
}
