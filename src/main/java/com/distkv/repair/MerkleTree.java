package com.distkv.repair;

import com.distkv.model.VersionedValue;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MerkleTree {
    private final String rootHash;
    private final Map<String, String> leafHashes;

    public MerkleTree(String rootHash, Map<String, String> leafHashes) {
        this.rootHash = rootHash;
        this.leafHashes = Map.copyOf(leafHashes);
    }

    public static MerkleTree fromSnapshot(Map<String, List<VersionedValue>> snapshot) {
        Map<String, String> leaves = new LinkedHashMap<>();
        snapshot.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> leaves.put(entry.getKey(), hashLeaf(entry.getKey(), entry.getValue())));
        return new MerkleTree(hashRoot(leaves), leaves);
    }

    public String rootHash() {
        return rootHash;
    }

    public Map<String, String> leafHashes() {
        return leafHashes;
    }

    public Set<String> differingKeys(MerkleTree other) {
        Set<String> keys = new LinkedHashSet<>();
        keys.addAll(leafHashes.keySet());
        keys.addAll(other.leafHashes.keySet());
        keys.removeIf(key -> leafHashes.getOrDefault(key, "").equals(other.leafHashes.getOrDefault(key, "")));
        return keys;
    }

    private static String hashRoot(Map<String, String> leaves) {
        MessageDigest digest = sha256();
        leaves.forEach((key, hash) -> {
            digest.update(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            digest.update(hash.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static String hashLeaf(String key, List<VersionedValue> versions) {
        MessageDigest digest = sha256();
        digest.update(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        versions.stream()
                .sorted(Comparator.comparingLong(VersionedValue::timestampEpochMs))
                .forEach(version -> {
                    digest.update(version.value());
                    digest.update(Long.toString(version.timestampEpochMs()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    digest.update(Boolean.toString(version.tombstone()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    version.vectorClock().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> {
                                digest.update(entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                                digest.update(Long.toString(entry.getValue()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                            });
                });
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 digest is not available", exception);
        }
    }
}
