package com.distkv.observability;

import com.distkv.membership.MemberInfo;
import com.distkv.membership.MemberStatus;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.hotspot.DefaultExports;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public final class DistKvMetrics implements AutoCloseable {
    private final CollectorRegistry registry = new CollectorRegistry();
    private final Counter opsTotal;
    private final Histogram latency;
    private final Counter quorumFailures;
    private final Gauge replicationLagMs;
    private final Gauge nodeHealth;
    private final Gauge walSizeBytes;
    private final Gauge pendingHints;
    private HTTPServer httpServer;

    public DistKvMetrics() {
        DefaultExports.initialize();
        this.opsTotal = Counter.build()
                .name("distkv_ops_total")
                .help("Total DistKV operations by operation type.")
                .labelNames("op")
                .register(registry);
        this.latency = Histogram.build()
                .name("distkv_latency_seconds")
                .help("DistKV request latency in seconds.")
                .labelNames("op")
                .buckets(0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0)
                .register(registry);
        this.quorumFailures = Counter.build()
                .name("distkv_quorum_failures_total")
                .help("Total quorum failures.")
                .register(registry);
        this.replicationLagMs = Gauge.build()
                .name("distkv_replication_lag_ms")
                .help("Oldest hinted handoff age in milliseconds.")
                .labelNames("node_id")
                .register(registry);
        this.nodeHealth = Gauge.build()
                .name("distkv_node_health")
                .help("Node health: 1 alive, 0.5 suspect, 0 dead.")
                .labelNames("node_id")
                .register(registry);
        this.walSizeBytes = Gauge.build()
                .name("distkv_wal_size_bytes")
                .help("WAL size in bytes.")
                .labelNames("node_id")
                .register(registry);
        this.pendingHints = Gauge.build()
                .name("distkv_pending_hints")
                .help("Number of hinted handoff writes queued locally.")
                .labelNames("node_id")
                .register(registry);
    }

    public void startHttpServer(int port) throws IOException {
        this.httpServer = new HTTPServer.Builder()
                .withPort(port)
                .withRegistry(registry)
                .build();
    }

    public <T> T recordOperation(String op, Supplier<T> operation) {
        Histogram.Timer timer = latency.labels(op).startTimer();
        try {
            return operation.get();
        } finally {
            timer.observeDuration();
            opsTotal.labels(op).inc();
        }
    }

    public void recordQuorumFailure() {
        quorumFailures.inc();
    }

    public void setNodeHealth(List<MemberInfo> members) {
        for (MemberInfo member : members) {
            nodeHealth.labels(member.endpoint().nodeId()).set(toHealthValue(member.status()));
        }
    }

    public void setWalSizeBytes(String nodeId, long bytes) {
        walSizeBytes.labels(nodeId).set(bytes);
    }

    public void setPendingHints(String nodeId, int count) {
        pendingHints.labels(nodeId).set(count);
    }

    public void setReplicationLagMs(String nodeId, long lagMs) {
        replicationLagMs.labels(nodeId).set(lagMs);
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.close();
        }
    }

    private double toHealthValue(MemberStatus status) {
        return switch (status) {
            case ALIVE -> 1.0;
            case SUSPECT -> 0.5;
            case DEAD -> 0.0;
        };
    }
}
