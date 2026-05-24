package com.distkv.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Objects;

public final class BloomFilter {
    private final BitSet bits;
    private final int bitSize;
    private final int hashCount;
    private final long expectedInsertions;
    private final double falsePositiveRate;

    public BloomFilter(long expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions < 1) {
            throw new IllegalArgumentException("expectedInsertions must be positive");
        }
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            throw new IllegalArgumentException("falsePositiveRate must be between 0 and 1");
        }
        this.expectedInsertions = expectedInsertions;
        this.falsePositiveRate = falsePositiveRate;
        this.bitSize = calculateBitSize(expectedInsertions, falsePositiveRate);
        this.hashCount = calculateHashCount(bitSize, expectedInsertions);
        this.bits = new BitSet(bitSize);
    }

    public void add(String key) {
        Objects.requireNonNull(key, "key");
        for (int index : indexesFor(key)) {
            bits.set(index);
        }
    }

    public boolean mightContain(String key) {
        Objects.requireNonNull(key, "key");
        for (int index : indexesFor(key)) {
            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }

    public int bitSize() {
        return bitSize;
    }

    public int hashCount() {
        return hashCount;
    }

    public long expectedInsertions() {
        return expectedInsertions;
    }

    public double configuredFalsePositiveRate() {
        return falsePositiveRate;
    }

    public static int calculateBitSize(long expectedInsertions, double falsePositiveRate) {
        return (int) Math.ceil(-(expectedInsertions * Math.log(falsePositiveRate)) / Math.pow(Math.log(2), 2));
    }

    public static int calculateHashCount(int bitSize, long expectedInsertions) {
        return Math.max(1, (int) Math.round((bitSize / (double) expectedInsertions) * Math.log(2)));
    }

    private int[] indexesFor(String key) {
        byte[] digest = sha256(key);
        ByteBuffer buffer = ByteBuffer.wrap(digest);
        long hash1 = buffer.getLong();
        long hash2 = buffer.getLong();
        if (hash2 == 0) {
            hash2 = 0x9E3779B97F4A7C15L;
        }
        int[] indexes = new int[hashCount];
        for (int index = 0; index < hashCount; index++) {
            long combined = hash1 + (index * hash2);
            indexes[index] = Math.floorMod(combined, bitSize);
        }
        return indexes;
    }

    private byte[] sha256(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(key.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 digest is not available", exception);
        }
    }
}
