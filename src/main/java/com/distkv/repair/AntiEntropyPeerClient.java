package com.distkv.repair;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;

import java.util.List;

public interface AntiEntropyPeerClient {
    MerkleTree fetchMerkle(NodeEndpoint peer);

    List<VersionedValue> fetchVersions(NodeEndpoint peer, String key);

    void applyVersions(NodeEndpoint peer, String key, List<VersionedValue> versions);
}
