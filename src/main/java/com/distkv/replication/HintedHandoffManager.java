package com.distkv.replication;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class HintedHandoffManager implements AutoCloseable {
    private final ReplicaClient replicaClient;
    private final Queue<Hint> hints = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService executor;
    private final Duration deliveryInterval;
    private ScheduledFuture<?> scheduledDelivery;

    public HintedHandoffManager(ReplicaClient replicaClient, Duration deliveryInterval) {
        this.replicaClient = Objects.requireNonNull(replicaClient, "replicaClient");
        this.deliveryInterval = Objects.requireNonNull(deliveryInterval, "deliveryInterval");
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "distkv-hinted-handoff");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (scheduledDelivery != null && !scheduledDelivery.isCancelled()) {
            return;
        }
        scheduledDelivery = executor.scheduleAtFixedRate(
                this::deliverHints,
                deliveryInterval.toMillis(),
                deliveryInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void storeHint(NodeEndpoint target, String key, VersionedValue value) {
        hints.add(new Hint(target, key, value, System.currentTimeMillis()));
    }

    public int pendingHintCount() {
        return hints.size();
    }

    public long oldestHintAgeMillis(long nowEpochMs) {
        return hints.stream()
                .mapToLong(hint -> Math.max(0, nowEpochMs - hint.storedAtEpochMs()))
                .max()
                .orElse(0L);
    }

    public void deliverHints() {
        int attempts = hints.size();
        for (int index = 0; index < attempts; index++) {
            Hint hint = hints.poll();
            if (hint == null) {
                return;
            }
            CompletableFuture<ReplicaWriteResult> delivery = replicaClient.write(hint.target(), hint.key(), hint.value());
            try {
                ReplicaWriteResult result = delivery.get(500, TimeUnit.MILLISECONDS);
                if (!result.acknowledged()) {
                    hints.add(hint);
                }
            } catch (Exception ignored) {
                hints.add(hint);
            }
        }
    }

    @Override
    public void close() {
        if (scheduledDelivery != null) {
            scheduledDelivery.cancel(true);
        }
        executor.shutdownNow();
    }

    public record Hint(NodeEndpoint target, String key, VersionedValue value, long storedAtEpochMs) {
    }
}
