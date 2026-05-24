package com.distkv.membership;

import com.distkv.model.NodeEndpoint;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class MembershipList {
    private final String localNodeId;
    private final Clock clock;
    private final Map<String, MutableMember> members = new ConcurrentHashMap<>();

    public MembershipList(NodeEndpoint localEndpoint, Clock clock) {
        this.localNodeId = Objects.requireNonNull(localEndpoint, "localEndpoint").nodeId();
        this.clock = Objects.requireNonNull(clock, "clock");
        members.put(localNodeId, new MutableMember(localEndpoint, new AtomicLong(0), clock.millis(), MemberStatus.ALIVE));
    }

    public void incrementLocalHeartbeat() {
        MutableMember local = members.get(localNodeId);
        local.heartbeat.incrementAndGet();
        local.lastSeenEpochMs = clock.millis();
        local.status = MemberStatus.ALIVE;
    }

    public void addOrMarkAlive(NodeEndpoint endpoint) {
        long now = clock.millis();
        members.compute(endpoint.nodeId(), (ignored, current) -> {
            if (current == null) {
                return new MutableMember(endpoint, new AtomicLong(0), now, MemberStatus.ALIVE);
            }
            current.endpoint = endpoint;
            current.lastSeenEpochMs = now;
            current.status = MemberStatus.ALIVE;
            return current;
        });
    }

    public boolean markDead(String nodeId) {
        MutableMember member = members.get(nodeId);
        if (member == null || nodeId.equals(localNodeId)) {
            return false;
        }
        member.status = MemberStatus.DEAD;
        member.lastSeenEpochMs = clock.millis();
        return true;
    }

    public Optional<MemberInfo> get(String nodeId) {
        return Optional.ofNullable(members.get(nodeId)).map(MutableMember::snapshot);
    }

    public List<MemberInfo> snapshot() {
        return members.values().stream()
                .map(MutableMember::snapshot)
                .sorted(Comparator.comparing(member -> member.endpoint().nodeId()))
                .toList();
    }

    public List<MemberInfo> healthyPeers() {
        return snapshot().stream()
                .filter(member -> !member.endpoint().nodeId().equals(localNodeId))
                .filter(member -> member.status() == MemberStatus.ALIVE)
                .toList();
    }

    public void merge(List<MemberInfo> incoming) {
        long now = clock.millis();
        for (MemberInfo remote : incoming) {
            if (remote.endpoint().nodeId().equals(localNodeId)) {
                continue;
            }
            members.compute(remote.endpoint().nodeId(), (ignored, current) -> {
                if (current == null) {
                    return new MutableMember(remote.endpoint(), new AtomicLong(remote.heartbeat()), now, remote.status());
                }
                if (remote.status() == MemberStatus.DEAD) {
                    current.status = MemberStatus.DEAD;
                    current.lastSeenEpochMs = now;
                    return current;
                }
                if (remote.heartbeat() > current.heartbeat.get()) {
                    current.endpoint = remote.endpoint();
                    current.heartbeat.set(remote.heartbeat());
                    current.lastSeenEpochMs = now;
                    current.status = MemberStatus.ALIVE;
                }
                return current;
            });
        }
    }

    public void markSuspects(long gossipIntervalMillis, int suspectAfterCycles) {
        long now = clock.millis();
        long suspectAfterMillis = gossipIntervalMillis * suspectAfterCycles;
        members.forEach((nodeId, member) -> {
            if (!nodeId.equals(localNodeId)
                    && member.status != MemberStatus.DEAD
                    && now - member.lastSeenEpochMs >= suspectAfterMillis) {
                member.status = MemberStatus.SUSPECT;
            }
        });
    }

    private static final class MutableMember {
        private volatile NodeEndpoint endpoint;
        private final AtomicLong heartbeat;
        private volatile long lastSeenEpochMs;
        private volatile MemberStatus status;

        private MutableMember(NodeEndpoint endpoint, AtomicLong heartbeat, long lastSeenEpochMs, MemberStatus status) {
            this.endpoint = endpoint;
            this.heartbeat = heartbeat;
            this.lastSeenEpochMs = lastSeenEpochMs;
            this.status = status;
        }

        private MemberInfo snapshot() {
            return new MemberInfo(endpoint, heartbeat.get(), lastSeenEpochMs, status);
        }
    }
}
