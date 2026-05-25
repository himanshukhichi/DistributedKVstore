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
import com.distkv.model.NodeEndpoint;
import com.distkv.observability.DistKvMetrics;
import com.distkv.replication.QuorumCoordinator;
import com.distkv.replication.ReadQuorumResult;
import com.distkv.replication.WriteQuorumResult;
import com.distkv.storage.KeyValueStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class KVServiceImpl extends KVServiceGrpc.KVServiceImplBase implements AutoCloseable {
    private static final long FORWARD_TIMEOUT_MILLIS = 750;

    private final QuorumCoordinator coordinator;
    private final KeyValueStore localStore;
    private final DistKvMetrics metrics;
    private final Map<String, ManagedChannel> forwardChannelsByNodeId = new ConcurrentHashMap<>();

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
            Optional<NodeEndpoint> remoteCoordinator = remoteCoordinatorFor(request.getKey());
            if (remoteCoordinator.isPresent()) {
                responseObserver.onNext(forwardStub(remoteCoordinator.get()).get(request));
                responseObserver.onCompleted();
                return;
            }
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
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            Optional<NodeEndpoint> remoteCoordinator = remoteCoordinatorFor(request.getKey());
            if (remoteCoordinator.isPresent()) {
                responseObserver.onNext(forwardStub(remoteCoordinator.get()).put(request));
                responseObserver.onCompleted();
                return;
            }
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
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            Optional<NodeEndpoint> remoteCoordinator = remoteCoordinatorFor(request.getKey());
            if (remoteCoordinator.isPresent()) {
                responseObserver.onNext(forwardStub(remoteCoordinator.get()).delete(request));
                responseObserver.onCompleted();
                return;
            }
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
        } catch (StatusRuntimeException exception) {
            responseObserver.onError(exception);
        } catch (RuntimeException exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(exception.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void scan(ScanRequest request, StreamObserver<Entry> responseObserver) {
        try {
            record("scan", () -> {
                // Scans carry a consistency level for API symmetry, but are local reads from this node's store.
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

    @Override
    public void close() {
        forwardChannelsByNodeId.values().forEach(channel -> {
            channel.shutdownNow();
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private <T> T record(String op, java.util.function.Supplier<T> operation) {
        return metrics == null ? operation.get() : metrics.recordOperation(op, operation);
    }

    private void recordQuorumFailure() {
        if (metrics != null) {
            metrics.recordQuorumFailure();
        }
    }

    private Optional<NodeEndpoint> remoteCoordinatorFor(String key) {
        return coordinator.coordinatorFor(key)
                .filter(node -> !node.nodeId().equals(coordinator.coordinatorNodeId()));
    }

    private KVServiceGrpc.KVServiceBlockingStub forwardStub(NodeEndpoint node) {
        ManagedChannel channel = forwardChannelsByNodeId.computeIfAbsent(node.nodeId(), ignored ->
                ManagedChannelBuilder.forAddress(node.host(), node.grpcPort())
                        .usePlaintext()
                        .build());
        return KVServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(FORWARD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }
}
