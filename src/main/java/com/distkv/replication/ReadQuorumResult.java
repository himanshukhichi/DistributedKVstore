package com.distkv.replication;

import com.distkv.model.VersionedValue;

import java.util.Optional;

public record ReadQuorumResult(boolean success, int replicasContacted, Optional<VersionedValue> value,
                               String message) {
}
