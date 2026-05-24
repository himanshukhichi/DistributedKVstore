package com.distkv.grpc;

import com.distkv.proto.DeleteRequest;
import com.distkv.proto.DeleteResponse;
import com.distkv.proto.Entry;
import com.distkv.proto.GetRequest;
import com.distkv.proto.GetResponse;
import com.distkv.proto.KVServiceGrpc;
import com.distkv.proto.PutRequest;
import com.distkv.proto.PutResponse;
import com.distkv.proto.ScanRequest;
import com.distkv.replication.QuorumCoordinator;
import com.distkv.replication.ReadQuorumResult;
import com.distkv.replication.WriteQuorumResult;
import com.distkv.storage.KeyValueStore;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

public final class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase {
    private final QuorumCoordinator coordinator;
    private final KeyValueStore localStore;

    public KVServiceImpl(QuorumCoordinator coordinator, KeyValueStore localStore) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.localStore = Objects.requireNonNull(localStore, "localStore");
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            ReadQuorumResult result = coordinator.get(request.getKey(), request.getConsistency());
            GetResponse.Builder response = GetResponse.newBuilder()
                    .setFound(result.value().isPresent())
                    .setMessage(result.message())
                    .setReplicasContacted(result.replicasContacted());
            result.value().ifPresent(value -> response.addVersions(ProtoMappers.toProto(value)));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            WriteQuorumResult result = coordinator.put(
                    request.getKey(),
                    request.getValue().toByteArray(),
                    request.getConsistency());
            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(result.success())
                    .setMessage(result.message())
                    .setAcksRequired(result.requiredAcks())
                    .setAcksReceived(result.receivedAcks())
                    .setVersion(ProtoMappers.toProto(result.version()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            WriteQuorumResult result = coordinator.delete(request.getKey(), request.getConsistency());
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setSuccess(result.success())
                    .setMessage(result.message())
                    .setAcksRequired(result.requiredAcks())
                    .setAcksReceived(result.receivedAcks())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<Entry> responseObserver) {
        try {
            localStore.scan(request.getStartKey(), request.getEndKey()).forEach(entry -> responseObserver.onNext(
                    Entry.newBuilder()
                            .setKey(entry.key())
                            .setVersion(ProtoMappers.toProto(entry.value()))
                            .build()));
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }
}
