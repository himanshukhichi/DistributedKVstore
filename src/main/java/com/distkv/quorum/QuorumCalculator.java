package com.distkv.quorum;

import com.distkv.proto.ConsistencyLevel;

public final class QuorumCalculator {
    private QuorumCalculator() {
    }

    public static int requiredResponses(int replicationFactor, ConsistencyLevel consistencyLevel) {
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("replicationFactor must be positive");
        }
        return switch (consistencyLevel) {
            case ONE, UNRECOGNIZED -> 1;
            case QUORUM -> (replicationFactor / 2) + 1;
            case ALL -> replicationFactor;
        };
    }

    public static boolean readWriteOverlap(int replicationFactor, ConsistencyLevel readConsistency,
                                           ConsistencyLevel writeConsistency) {
        int requiredReads = requiredResponses(replicationFactor, readConsistency);
        int requiredWrites = requiredResponses(replicationFactor, writeConsistency);
        return requiredReads + requiredWrites > replicationFactor;
    }
}
