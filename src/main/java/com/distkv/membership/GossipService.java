package com.distkv.membership;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class GossipService implements AutoCloseable {
    public static final int DEFAULT_FANOUT = 2;
    public static final int DEFAULT_SUSPECT_AFTER_CYCLES = 3;

    private final MembershipList membershipList;
    private final GossipPeerClient peerClient;
    private final Duration interval;
    private final int fanout;
    private final int suspectAfterCycles;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledTask;

    public GossipService(MembershipList membershipList, GossipPeerClient peerClient) {
        this(membershipList, peerClient, Duration.ofSeconds(1), DEFAULT_FANOUT, DEFAULT_SUSPECT_AFTER_CYCLES);
    }

    public GossipService(MembershipList membershipList, GossipPeerClient peerClient, Duration interval,
                         int fanout, int suspectAfterCycles) {
        this.membershipList = Objects.requireNonNull(membershipList, "membershipList");
        this.peerClient = Objects.requireNonNull(peerClient, "peerClient");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.fanout = fanout;
        this.suspectAfterCycles = suspectAfterCycles;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "distkv-gossip");
            thread.setDaemon(true);
            return thread;
        });
    }

    public synchronized void start() {
        if (scheduledTask != null && !scheduledTask.isCancelled()) {
            return;
        }
        scheduledTask = executor.scheduleAtFixedRate(this::gossipOnce, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void gossipOnce() {
        membershipList.incrementLocalHeartbeat();
        List<MemberInfo> peers = new ArrayList<>(membershipList.healthyPeers());
        Collections.shuffle(peers);
        peers.stream().limit(fanout).forEach(peer -> {
            try {
                List<MemberInfo> remoteMembership = peerClient.exchange(peer.endpoint(), membershipList.snapshot());
                membershipList.merge(remoteMembership);
            } catch (Exception ignored) {
                // Missed gossip rounds are converted to SUSPECT by markSuspects.
            }
        });
        membershipList.markSuspects(interval.toMillis(), suspectAfterCycles);
    }

    @Override
    public synchronized void close() {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }
        executor.shutdownNow();
    }
}
