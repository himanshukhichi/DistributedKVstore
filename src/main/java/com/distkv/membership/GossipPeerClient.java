package com.distkv.membership;

import com.distkv.model.NodeEndpoint;

import java.util.List;

public interface GossipPeerClient {
    List<MemberInfo> exchange(NodeEndpoint peer, List<MemberInfo> localMembership) throws Exception;
}
