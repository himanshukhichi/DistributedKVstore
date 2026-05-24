package com.distkv.membership;

import com.distkv.model.NodeEndpoint;

public record MemberInfo(NodeEndpoint endpoint, long heartbeat, long lastSeenEpochMs, MemberStatus status) {
    public boolean healthy() {
        return status == MemberStatus.ALIVE;
    }
}
