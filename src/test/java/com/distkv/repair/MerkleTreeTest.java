package com.distkv.repair;

import com.distkv.model.VersionedValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MerkleTreeTest {
    @Test
    void identifiesDivergedKeysByLeafHash() {
        MerkleTree left = MerkleTree.fromSnapshot(Map.of(
                "alpha", List.of(new VersionedValue("one".getBytes(), 1, Map.of("node-a", 1L), false)),
                "beta", List.of(new VersionedValue("two".getBytes(), 1, Map.of("node-a", 1L), false))));
        MerkleTree right = MerkleTree.fromSnapshot(Map.of(
                "alpha", List.of(new VersionedValue("one".getBytes(), 1, Map.of("node-a", 1L), false)),
                "beta", List.of(new VersionedValue("changed".getBytes(), 2, Map.of("node-b", 1L), false)),
                "gamma", List.of(new VersionedValue("three".getBytes(), 1, Map.of("node-b", 1L), false))));

        assertEquals(2, left.differingKeys(right).size());
        assertTrue(left.differingKeys(right).contains("beta"));
        assertTrue(left.differingKeys(right).contains("gamma"));
    }
}
