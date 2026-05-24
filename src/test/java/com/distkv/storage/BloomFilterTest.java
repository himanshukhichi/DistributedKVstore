package com.distkv.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {
    @Test
    void calculatesExpectedBitAndHashCounts() {
        BloomFilter filter = new BloomFilter(1_000, 0.01);

        assertEquals(9_586, filter.bitSize());
        assertEquals(7, filter.hashCount());
    }

    @Test
    void reportsNoFalseNegativesAndLowFalsePositiveRate() {
        BloomFilter filter = new BloomFilter(1_000, 0.01);
        for (int index = 0; index < 1_000; index++) {
            filter.add("present-" + index);
        }

        for (int index = 0; index < 1_000; index++) {
            assertTrue(filter.mightContain("present-" + index));
        }

        int falsePositives = 0;
        int trials = 10_000;
        for (int index = 0; index < trials; index++) {
            if (filter.mightContain("absent-" + index)) {
                falsePositives++;
            }
        }

        double falsePositiveRate = falsePositives / (double) trials;
        assertTrue(falsePositiveRate < 0.025, "false positive rate was " + falsePositiveRate);
        assertFalse(filter.mightContain("definitely-missing-sentinel"));
    }
}
