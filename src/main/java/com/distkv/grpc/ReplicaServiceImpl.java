package com.distkv.grpc;

import com.distkv.proto.ReplicaApplyRequest;
import com.distkv.proto.ReplicaApplyResponse;
import com.distkv.proto.ReplicaApplyVersionsRequest;
import com.distkv.proto.FetchVersionsRequest;
import com.distkv.proto.FetchVersionsResponse;
import com.distkv.proto.MerkleRequest;
import com.distkv.proto.MerkleResponse;
import com.distkv.proto.ReplicaReadRequest;
import com.distkv.proto.ReplicaReadResponse;
import com.distkv.proto.ReplicaServiceGrpc;
import com.distkv.repair.MerkleTree;
import com.distkv.storage.KeyValueStore;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

public final class ReplicaServiceImpl extends ReplicaServiceGrpc.ReplicaServiceImplBase {
    private final KeyValueStore store;

    public ReplicaServiceImpl(KeyValueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void apply(ReplicaApplyRequest request, StreamObserver<ReplicaApplyResponse> responseObserver) {
        try {
            store.apply(request.getKey(), ProtoMappers.fromProto(request.getVersion()));
            responseObserver.onNext(ReplicaApplyResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("applied")
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void applyVersions(ReplicaApplyVersionsRequest request, StreamObserver<ReplicaApplyResponse> responseObserver) {
        try {
            request.getVersionsList().stream()
                    .map(ProtoMappers::fromProto)
                    .forEach(version -> store.apply(request.getKey(), version));
            responseObserver.onNext(ReplicaApplyResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("versions applied")
                    .build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void read(ReplicaReadRequest request, StreamObserver<ReplicaReadResponse> responseObserver) {
        try {
            ReplicaReadResponse.Builder response = ReplicaReadResponse.newBuilder()
                    .setResponded(true)
                    .setMessage("read complete");
            java.util.List<com.distkv.model.VersionedValue> versions = store.getVersionsIncludingTombstone(request.getKey());
            if (versions.isEmpty()) {
                response.setFound(false);
            } else {
                response.setFound(true)
                        .setVersion(ProtoMappers.toProto(com.distkv.model.VersionedValue.latestIncludingTombstone(versions)))
                        .addAllVersions(versions.stream().map(ProtoMappers::toProto).toList());
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void merkle(MerkleRequest request, StreamObserver<MerkleResponse> responseObserver) {
        MerkleTree tree = MerkleTree.fromSnapshot(store.snapshotVersions());
        responseObserver.onNext(MerkleResponse.newBuilder()
                .setRootHash(tree.rootHash())
                .putAllLeafHashes(tree.leafHashes())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void fetchVersions(FetchVersionsRequest request, StreamObserver<FetchVersionsResponse> responseObserver) {
        responseObserver.onNext(FetchVersionsResponse.newBuilder()
                .addAllVersions(store.getVersionsIncludingTombstone(request.getKey()).stream()
                        .map(ProtoMappers::toProto)
                        .toList())
                .build());
        responseObserver.onCompleted();
    }
}
