package com.distkv.replication;

import com.distkv.model.VersionedValue;

import java.util.List;
import java.util.Optional;

public record ReadQuorumResult(boolean success, int replicasContacted, List<VersionedValue> versions,
                               String message) {
    public ReadQuorumResult {
        versions = List.copyOf(versions);
    }

    public Optional<VersionedValue> value() {
        VersionedValue latest = VersionedValue.latestIncludingTombstone(versions);
        if (latest == null || latest.tombstone()) {
            return Optional.empty();
        }
        return Optional.of(latest);
    }
}
