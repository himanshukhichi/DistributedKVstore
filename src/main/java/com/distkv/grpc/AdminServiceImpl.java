package com.distkv.grpc;

import com.distkv.membership.MembershipList;
import com.distkv.model.NodeEndpoint;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.ClusterStatusRequest;
import com.distkv.proto.ClusterStatusResponse;
import com.distkv.proto.NodeJoinRequest;
import com.distkv.proto.NodeJoinResponse;
import com.distkv.proto.NodeLeaveRequest;
import com.distkv.proto.NodeLeaveResponse;
import com.distkv.routing.ConsistentHashRing;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

public final class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {
    private final MembershipList membershipList;
    private final ConsistentHashRing ring;

    public AdminServiceImpl(MembershipList membershipList, ConsistentHashRing ring) {
        this.membershipList = Objects.requireNonNull(membershipList, "membershipList");
        this.ring = Objects.requireNonNull(ring, "ring");
    }

    @Override
    public void clusterStatus(ClusterStatusRequest request, StreamObserver<ClusterStatusResponse> responseObserver) {
        ClusterStatusResponse response = ClusterStatusResponse.newBuilder()
                .addAllNodes(membershipList.snapshot().stream().map(ProtoMappers::toProto).toList())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void nodeJoin(NodeJoinRequest request, StreamObserver<NodeJoinResponse> responseObserver) {
        try {
            NodeEndpoint endpoint = ProtoMappers.fromProto(request.getNode());
            membershipList.addOrMarkAlive(endpoint);
            ring.addNode(endpoint);
            responseObserver.onNext(NodeJoinResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("node joined: " + endpoint.nodeId())
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void nodeLeave(NodeLeaveRequest request, StreamObserver<NodeLeaveResponse> responseObserver) {
        boolean memberUpdated = membershipList.markDead(request.getNodeId());
        boolean ringUpdated = ring.removeNode(request.getNodeId());
        responseObserver.onNext(NodeLeaveResponse.newBuilder()
                .setSuccess(memberUpdated || ringUpdated)
                .setMessage((memberUpdated || ringUpdated)
                        ? "node left: " + request.getNodeId()
                        : "node not found: " + request.getNodeId())
                .build());
        responseObserver.onCompleted();
    }
}
