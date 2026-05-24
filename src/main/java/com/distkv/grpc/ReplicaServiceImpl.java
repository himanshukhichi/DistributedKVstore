package com.distkv.grpc;

import com.distkv.proto.ReplicaApplyRequest;
import com.distkv.proto.ReplicaApplyResponse;
import com.distkv.proto.ReplicaReadRequest;
import com.distkv.proto.ReplicaReadResponse;
import com.distkv.proto.ReplicaServiceGrpc;
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
    public void read(ReplicaReadRequest request, StreamObserver<ReplicaReadResponse> responseObserver) {
        try {
            ReplicaReadResponse.Builder response = ReplicaReadResponse.newBuilder()
                    .setResponded(true)
                    .setMessage("read complete");
            store.getIncludingTombstone(request.getKey()).ifPresentOrElse(
                    value -> response.setFound(true).setVersion(ProtoMappers.toProto(value)),
                    () -> response.setFound(false));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }
}
