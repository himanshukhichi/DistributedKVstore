package com.distkv.repair;

import com.distkv.model.NodeEndpoint;
import com.distkv.model.VersionedValue;
import com.distkv.storage.KeyValueStore;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class AntiEntropyService implements AutoCloseable {
    private final KeyValueStore localStore;
    private final Supplier<List<NodeEndpoint>> peersSupplier;
    private final AntiEntropyPeerClient peerClient;
    private final Duration interval;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledRepair;

    public AntiEntropyService(KeyValueStore localStore, Supplier<List<NodeEndpoint>> peersSupplier,
                              AntiEntropyPeerClient peerClient, Duration interval) {
        this.localStore = Objects.requireNonNull(localStore, "localStore");
        this.peersSupplier = Objects.requireNonNull(peersSupplier, "peersSupplier");
        this.peerClient = Objects.requireNonNull(peerClient, "peerClient");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "distkv-anti-entropy");
            thread.setDaemon(true);
            return thread;
        });
    }

    public synchronized void start() {
        if (scheduledRepair != null && !scheduledRepair.isCancelled()) {
            return;
        }
        scheduledRepair = executor.scheduleAtFixedRate(
                this::repairOnce,
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public void repairOnce() {
        MerkleTree localTree = MerkleTree.fromSnapshot(localStore.snapshotVersions());
        for (NodeEndpoint peer : peersSupplier.get()) {
            try {
                MerkleTree remoteTree = peerClient.fetchMerkle(peer);
                if (localTree.rootHash().equals(remoteTree.rootHash())) {
                    continue;
                }
                repairPeer(peer, localTree.differingKeys(remoteTree));
            } catch (RuntimeException ignored) {
                // Failed repairs are retried on the next anti-entropy pass.
            }
        }
    }

    @Override
    public synchronized void close() {
        if (scheduledRepair != null) {
            scheduledRepair.cancel(true);
        }
        executor.shutdownNow();
    }

    private void repairPeer(NodeEndpoint peer, Set<String> differingKeys) {
        for (String key : differingKeys) {
            List<VersionedValue> remoteVersions = peerClient.fetchVersions(peer, key);
            remoteVersions.forEach(version -> localStore.apply(key, version));

            List<VersionedValue> localVersions = localStore.getVersionsIncludingTombstone(key);
            if (!localVersions.isEmpty()) {
                peerClient.applyVersions(peer, key, localVersions);
            }
        }
    }
}
