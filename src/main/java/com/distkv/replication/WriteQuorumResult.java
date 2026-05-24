package com.distkv.replication;

import com.distkv.model.VersionedValue;

public record WriteQuorumResult(boolean success, int requiredAcks, int receivedAcks,
                                String message, VersionedValue version) {
}
