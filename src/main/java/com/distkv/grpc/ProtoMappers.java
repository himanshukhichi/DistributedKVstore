package com.distkv.grpc;

import com.distkv.membership.MemberInfo;
import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.proto.NodeInfo;
import com.distkv.proto.ValueVersion;
import com.google.protobuf.ByteString;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ProtoMappers {
    private ProtoMappers() {
    }

    public static ValueVersion toProto(VersionedValue value) {
        return ValueVersion.newBuilder()
                .setValue(ByteString.copyFrom(value.value()))
                .setTimestampEpochMs(value.timestampEpochMs())
                .putAllVectorClock(value.vectorClock())
                .setTombstone(value.tombstone())
                .build();
    }

    public static VersionedValue fromProto(ValueVersion value) {
        Map<String, Long> vectorClock = new LinkedHashMap<>(value.getVectorClockMap());
        return new VersionedValue(
                value.getValue().toByteArray(),
                value.getTimestampEpochMs(),
                vectorClock,
                value.getTombstone());
    }

    public static NodeInfo toProto(MemberInfo member) {
        return NodeInfo.newBuilder()
                .setNodeId(member.endpoint().nodeId())
                .setHost(member.endpoint().host())
                .setGrpcPort(member.endpoint().grpcPort())
                .setHealthy(member.healthy())
                .setStatus(member.status().name())
                .setHeartbeat(member.heartbeat())
                .setLastSeenEpochMs(member.lastSeenEpochMs())
                .build();
    }

    public static NodeEndpoint fromProto(NodeInfo nodeInfo) {
        return new NodeEndpoint(nodeInfo.getNodeId(), nodeInfo.getHost(), nodeInfo.getGrpcPort());
    }
}
