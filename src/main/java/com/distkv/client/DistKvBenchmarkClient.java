package com.distkv.client;

import com.distkv.model.NodeEndpoint;
import com.distkv.proto.AdminServiceGrpc;
import com.distkv.proto.ClusterStatusRequest;
import com.distkv.proto.ConsistencyLevel;
import com.distkv.proto.GetRequest;
import com.distkv.proto.KVServiceGrpc;
import com.distkv.proto.PutRequest;
import com.distkv.routing.ConsistentHashRing;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class DistKvBenchmarkClient {
    private DistKvBenchmarkClient() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        List<Target> targets = parseTargets(config.nodes);
        try (Benchmark benchmark = new Benchmark(config, targets)) {
            benchmark.printClusterStatus();
            if (config.warmupSeconds > 0) {
                benchmark.runPhase("warmup", config.warmupSeconds, false);
            }
            BenchmarkResult result = benchmark.runPhase("measure", config.durationSeconds, true);
            result.print(config);
        }
    }

    private static List<Target> parseTargets(String rawNodes) {
        if (rawNodes == null || rawNodes.isBlank()) {
            throw new IllegalArgumentException("--nodes is required");
        }
        List<Target> targets = Arrays.stream(rawNodes.split(","))
                .map(String::trim)
                .filter(node -> !node.isBlank())
                .map(Target::parse)
                .toList();
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("--nodes must contain at least one node");
        }
        return targets;
    }

    private static final class Benchmark implements AutoCloseable {
        private final Config config;
        private final List<Target> targets;
        private final List<ManagedChannel> channels;
        private final ConsistentHashRing ring = new ConsistentHashRing();

        private Benchmark(Config config, List<Target> targets) {
            this.config = config;
            this.targets = List.copyOf(targets);
            this.channels = targets.stream()
                    .map(target -> ManagedChannelBuilder.forAddress(target.host, target.port)
                            .usePlaintext()
                            .build())
                    .toList();
            targets.forEach(target -> ring.addNode(new NodeEndpoint(target.nodeId, target.host, target.port)));
        }

        private void printClusterStatus() {
            try {
                var response = AdminServiceGrpc.newBlockingStub(channels.get(0))
                        .withDeadlineAfter(config.rpcTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        .clusterStatus(ClusterStatusRequest.newBuilder().build());
                System.out.printf("CLUSTER nodes_seen=%d contacted=%s%n",
                        response.getNodesCount(), targets.get(0).nodeId);
                response.getNodesList().stream()
                        .sorted(Comparator.comparing(node -> node.getNodeId()))
                        .forEach(node -> System.out.printf("CLUSTER_NODE node_id=%s host=%s port=%d healthy=%s status=%s%n",
                                node.getNodeId(),
                                node.getHost(),
                                node.getGrpcPort(),
                                node.getHealthy(),
                                node.getStatus()));
            } catch (RuntimeException exception) {
                System.out.printf("CLUSTER status_check_failed=%s%n", exception.getMessage());
            }
        }

        private BenchmarkResult runPhase(String phase, int seconds, boolean recordLatencies) throws Exception {
            CountDownLatch start = new CountDownLatch(1);
            long durationNanos = Duration.ofSeconds(seconds).toNanos();
            List<Future<WorkerResult>> futures = new ArrayList<>(config.concurrency);
            long createdAt = System.nanoTime();
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int workerId = 0; workerId < config.concurrency; workerId++) {
                    int id = workerId;
                    futures.add(executor.submit(() -> runWorker(id, phase, durationNanos, start, recordLatencies)));
                }
                long startNanos = System.nanoTime();
                start.countDown();
                List<WorkerResult> workers = new ArrayList<>(config.concurrency);
                for (Future<WorkerResult> future : futures) {
                    workers.add(future.get());
                }
                long endNanos = System.nanoTime();
                BenchmarkResult result = BenchmarkResult.from(workers, endNanos - startNanos);
                if (!recordLatencies) {
                    System.out.printf(Locale.ROOT,
                            "WARMUP seconds=%d successes=%d failures=%d throughput_ops_sec=%.2f%n",
                            seconds,
                            result.successes,
                            result.failures,
                            result.throughputOpsPerSecond());
                }
                return result;
            } finally {
                long startupMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - createdAt);
                if (recordLatencies) {
                    System.out.printf("BENCHMARK_PHASE phase=%s setup_and_run_ms=%d%n", phase, startupMillis);
                }
            }
        }

        private WorkerResult runWorker(int workerId, String phase, long durationNanos, CountDownLatch start,
                                       boolean recordLatencies) throws InterruptedException {
            start.await();
            long deadline = System.nanoTime() + durationNanos;
            byte[] value = valueBytes(config.valueBytes);
            ByteString byteString = ByteString.copyFrom(value);
            Latencies latencies = recordLatencies ? new Latencies(4096) : null;
            long successes = 0;
            long failures = 0;
            long sequence = 0;
            while (System.nanoTime() < deadline) {
                String key = config.keyPrefix + "-" + phase + "-" + workerId + "-" + sequence++;
                Target target = targetFor(key);
                KVServiceGrpc.KVServiceBlockingStub stub = KVServiceGrpc.newBlockingStub(channels.get(target.index))
                        .withDeadlineAfter(config.rpcTimeout.toMillis(), TimeUnit.MILLISECONDS);
                long started = System.nanoTime();
                try {
                    boolean success = switch (config.operation) {
                        case PUT -> stub.put(PutRequest.newBuilder()
                                .setKey(key)
                                .setValue(byteString)
                                .setConsistency(config.consistency)
                                .build()).getSuccess();
                        case GET -> stub.get(GetRequest.newBuilder()
                                .setKey(key)
                                .setConsistency(config.consistency)
                                .build()).getFound();
                    };
                    long elapsed = System.nanoTime() - started;
                    if (success) {
                        successes++;
                        if (latencies != null) {
                            latencies.add(elapsed);
                        }
                    } else {
                        failures++;
                    }
                } catch (StatusRuntimeException exception) {
                    failures++;
                }
            }
            return new WorkerResult(successes, failures, latencies == null ? new long[0] : latencies.copy());
        }

        private Target targetFor(String key) {
            if (config.routing == Routing.RANDOM) {
                return targets.get(ThreadLocalRandom.current().nextInt(targets.size()));
            }
            NodeEndpoint coordinator = ring.getPreferenceList(key, config.replicationFactor).get(0);
            for (Target target : targets) {
                if (target.nodeId.equals(coordinator.nodeId())) {
                    return target;
                }
            }
            throw new IllegalStateException("coordinator target not found: " + coordinator.nodeId());
        }

        @Override
        public void close() {
            channels.forEach(channel -> {
                channel.shutdownNow();
                try {
                    channel.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    private static byte[] valueBytes(int size) {
        byte[] value = new byte[size];
        byte[] seed = "distkv-benchmark-value".getBytes(StandardCharsets.UTF_8);
        for (int index = 0; index < value.length; index++) {
            value[index] = seed[index % seed.length];
        }
        return value;
    }

    private record WorkerResult(long successes, long failures, long[] latenciesNanos) {
    }

    private static final class BenchmarkResult {
        private final long successes;
        private final long failures;
        private final long elapsedNanos;
        private final long[] latenciesNanos;

        private BenchmarkResult(long successes, long failures, long elapsedNanos, long[] latenciesNanos) {
            this.successes = successes;
            this.failures = failures;
            this.elapsedNanos = elapsedNanos;
            this.latenciesNanos = latenciesNanos;
        }

        private static BenchmarkResult from(List<WorkerResult> workers, long elapsedNanos) {
            long successes = 0;
            long failures = 0;
            int sampleCount = 0;
            for (WorkerResult worker : workers) {
                successes += worker.successes();
                failures += worker.failures();
                sampleCount += worker.latenciesNanos().length;
            }
            long[] latencies = new long[sampleCount];
            int offset = 0;
            for (WorkerResult worker : workers) {
                System.arraycopy(worker.latenciesNanos(), 0, latencies, offset, worker.latenciesNanos().length);
                offset += worker.latenciesNanos().length;
            }
            Arrays.sort(latencies);
            return new BenchmarkResult(successes, failures, elapsedNanos, latencies);
        }

        private double throughputOpsPerSecond() {
            return successes / (elapsedNanos / 1_000_000_000.0);
        }

        private void print(Config config) {
            boolean claimValid = throughputOpsPerSecond() >= 50_000.0 && percentileMillis(0.99) < 10.0;
            System.out.printf(Locale.ROOT,
                    "BENCHMARK_RESULT operation=%s consistency=%s routing=%s nodes=%d replication_factor=%d " +
                            "concurrency=%d duration_seconds=%d value_bytes=%d successes=%d failures=%d " +
                            "throughput_ops_sec=%.2f p50_ms=%.3f p90_ms=%.3f p95_ms=%.3f p99_ms=%.3f max_ms=%.3f " +
                            "claim_50k_sub10ms_valid=%s%n",
                    config.operation,
                    config.consistency,
                    config.routing,
                    config.nodeCount(),
                    config.replicationFactor,
                    config.concurrency,
                    config.durationSeconds,
                    config.valueBytes,
                    successes,
                    failures,
                    throughputOpsPerSecond(),
                    percentileMillis(0.50),
                    percentileMillis(0.90),
                    percentileMillis(0.95),
                    percentileMillis(0.99),
                    maxMillis(),
                    claimValid);
        }

        private double percentileMillis(double percentile) {
            if (latenciesNanos.length == 0) {
                return Double.NaN;
            }
            int index = Math.max(0, (int) Math.ceil(percentile * latenciesNanos.length) - 1);
            return latenciesNanos[index] / 1_000_000.0;
        }

        private double maxMillis() {
            if (latenciesNanos.length == 0) {
                return Double.NaN;
            }
            return latenciesNanos[latenciesNanos.length - 1] / 1_000_000.0;
        }
    }

    private static final class Latencies {
        private long[] values;
        private int size;

        private Latencies(int initialCapacity) {
            this.values = new long[initialCapacity];
        }

        private void add(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        private long[] copy() {
            return Arrays.copyOf(values, size);
        }
    }

    private record Target(int index, String nodeId, String host, int port) {
        private static int nextIndex;

        private static Target parse(String raw) {
            String[] parts = raw.split(":");
            if (parts.length == 2) {
                int index = nextIndex++;
                return new Target(index, "node-" + (index + 1), parts[0], Integer.parseInt(parts[1]));
            }
            if (parts.length == 3) {
                int index = nextIndex++;
                return new Target(index, parts[0], parts[1], Integer.parseInt(parts[2]));
            }
            throw new IllegalArgumentException("node must use host:port or nodeId:host:port format: " + raw);
        }
    }

    private enum Operation {
        PUT,
        GET
    }

    private enum Routing {
        RANDOM,
        COORDINATOR
    }

    private static final class Config {
        private String nodes;
        private Operation operation = Operation.PUT;
        private ConsistencyLevel consistency = ConsistencyLevel.QUORUM;
        private Routing routing = Routing.COORDINATOR;
        private int replicationFactor = 3;
        private int concurrency = 128;
        private int durationSeconds = 20;
        private int warmupSeconds = 5;
        private int valueBytes = 64;
        private String keyPrefix = "bench";
        private Duration rpcTimeout = Duration.ofSeconds(2);

        private static Config parse(String[] args) {
            Config config = new Config();
            for (int index = 0; index < args.length; index++) {
                String arg = args[index];
                String value = index + 1 < args.length ? args[index + 1] : null;
                switch (arg) {
                    case "--nodes" -> config.nodes = requireValue(arg, value, ++index);
                    case "--operation" -> config.operation = Operation.valueOf(requireValue(arg, value, ++index).toUpperCase(Locale.ROOT));
                    case "--consistency" -> config.consistency = ConsistencyLevel.valueOf(requireValue(arg, value, ++index).toUpperCase(Locale.ROOT));
                    case "--routing" -> config.routing = Routing.valueOf(requireValue(arg, value, ++index).toUpperCase(Locale.ROOT));
                    case "--replication-factor" -> config.replicationFactor = Integer.parseInt(requireValue(arg, value, ++index));
                    case "--concurrency" -> config.concurrency = Integer.parseInt(requireValue(arg, value, ++index));
                    case "--duration-seconds" -> config.durationSeconds = Integer.parseInt(requireValue(arg, value, ++index));
                    case "--warmup-seconds" -> config.warmupSeconds = Integer.parseInt(requireValue(arg, value, ++index));
                    case "--value-bytes" -> config.valueBytes = Integer.parseInt(requireValue(arg, value, ++index));
                    case "--key-prefix" -> config.keyPrefix = requireValue(arg, value, ++index);
                    case "--rpc-timeout-ms" -> config.rpcTimeout = Duration.ofMillis(Long.parseLong(requireValue(arg, value, ++index)));
                    default -> throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            config.validate();
            return config;
        }

        private static String requireValue(String arg, String value, int ignoredIndex) {
            if (value == null || value.startsWith("--")) {
                throw new IllegalArgumentException(arg + " requires a value");
            }
            return value;
        }

        private void validate() {
            Objects.requireNonNull(nodes, "--nodes is required");
            if (replicationFactor < 1) {
                throw new IllegalArgumentException("--replication-factor must be positive");
            }
            if (concurrency < 1) {
                throw new IllegalArgumentException("--concurrency must be positive");
            }
            if (durationSeconds < 1) {
                throw new IllegalArgumentException("--duration-seconds must be positive");
            }
            if (warmupSeconds < 0) {
                throw new IllegalArgumentException("--warmup-seconds cannot be negative");
            }
            if (valueBytes < 1) {
                throw new IllegalArgumentException("--value-bytes must be positive");
            }
        }

        private int nodeCount() {
            return (int) Arrays.stream(nodes.split(",")).filter(node -> !node.isBlank()).count();
        }
    }
}
