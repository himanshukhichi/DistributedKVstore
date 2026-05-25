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
import com.distkv.observability.DistKvMetrics;
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
    private final DistKvMetrics metrics;

    public KVServiceImpl(QuorumCoordinator coordinator, KeyValueStore localStore) {
        this(coordinator, localStore, null);
    }

    public KVServiceImpl(QuorumCoordinator coordinator, KeyValueStore localStore, DistKvMetrics metrics) {
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.localStore = Objects.requireNonNull(localStore, "localStore");
        this.metrics = metrics;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            ReadQuorumResult result = record("get", () -> coordinator.get(request.getKey(), request.getConsistency()));
            if (!result.success()) {
                recordQuorumFailure();
            }
            GetResponse.Builder response = GetResponse.newBuilder()
                    .setFound(result.value().isPresent())
                    .setMessage(result.message())
                    .setReplicasContacted(result.replicasContacted());
            response.addAllVersions(result.versions().stream().map(ProtoMappers::toProto).toList());
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            WriteQuorumResult result = record("put", () -> coordinator.put(
                    request.getKey(),
                    request.getValue().toByteArray(),
                    request.getConsistency()));
            if (!result.success()) {
                recordQuorumFailure();
            }
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
            WriteQuorumResult result = record("delete", () -> coordinator.delete(request.getKey(), request.getConsistency()));
            if (!result.success()) {
                recordQuorumFailure();
            }
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
            record("scan", () -> {
                // Scans are always local reads from this node's store.
                // For distributed range queries, clients should perform local scans on any node
                // and filter/merge results if needed across multiple nodes.
                localStore.scan(request.getStartKey(), request.getEndKey()).forEach(entry -> responseObserver.onNext(
                        Entry.newBuilder()
                                .setKey(entry.key())
                                .setVersion(ProtoMappers.toProto(entry.value()))
                                .build()));
                return null;
            });
            responseObserver.onCompleted();
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    private <T> T record(String op, java.util.function.Supplier<T> operation) {
        return metrics == null ? operation.get() : metrics.recordOperation(op, operation);
    }

    private void recordQuorumFailure() {
        if (metrics != null) {
            metrics.recordQuorumFailure();
        }
    }
}
