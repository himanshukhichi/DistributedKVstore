package com.distkv.storage;

import com.distkv.model.VersionedValue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface KeyValueStore {
    VersionedValue put(String key, byte[] value, String nodeId, long counter);

    VersionedValue delete(String key, String nodeId, long counter);

    void apply(String key, VersionedValue value);

    Optional<VersionedValue> get(String key);

    Optional<VersionedValue> getIncludingTombstone(String key);

    List<VersionedValue> getVersions(String key);

    List<VersionedValue> getVersionsIncludingTombstone(String key);

    List<StoredEntry> scan(String startKeyInclusive, String endKeyExclusive);

    Map<String, VersionedValue> snapshot();

    Map<String, List<VersionedValue>> snapshotVersions();
}
