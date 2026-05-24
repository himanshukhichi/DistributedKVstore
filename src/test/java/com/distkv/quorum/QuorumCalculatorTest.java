package com.distkv.quorum;

import com.distkv.proto.ConsistencyLevel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuorumCalculatorTest {
    @Test
    void mapsConsistencyLevelsToRequiredResponses() {
        assertEquals(1, QuorumCalculator.requiredResponses(3, ConsistencyLevel.ONE));
        assertEquals(2, QuorumCalculator.requiredResponses(3, ConsistencyLevel.QUORUM));
        assertEquals(3, QuorumCalculator.requiredResponses(3, ConsistencyLevel.ALL));
    }

    @Test
    void identifiesReadWriteOverlapCombinations() {
        assertFalse(QuorumCalculator.readWriteOverlap(3, ConsistencyLevel.ONE, ConsistencyLevel.ONE));
        assertFalse(QuorumCalculator.readWriteOverlap(3, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM));
        assertTrue(QuorumCalculator.readWriteOverlap(3, ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM));
        assertTrue(QuorumCalculator.readWriteOverlap(3, ConsistencyLevel.ALL, ConsistencyLevel.ONE));
    }
}
