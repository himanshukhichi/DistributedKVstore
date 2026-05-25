package com.distkv.storage;

import com.distkv.model.VersionedValue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class WALManager {
    private static final int MAGIC = 0x44565741;
    private static final int SNAPSHOT_MAGIC = 0x4456534E;
    private static final byte VERSION = 1;

    private final Path walPath;
    private final Path snapshotPath;
    private final long compactionThresholdWrites;
    private long writesSinceCompaction;

    public WALManager(Path walPath) throws IOException {
        this(walPath, walPath.resolveSibling(walPath.getFileName() + ".snapshot"), Long.MAX_VALUE);
    }

    public WALManager(Path walPath, long compactionThresholdWrites) throws IOException {
        this(walPath, walPath.resolveSibling(walPath.getFileName() + ".snapshot"), compactionThresholdWrites);
    }

    public WALManager(Path walPath, Path snapshotPath, long compactionThresholdWrites) throws IOException {
        this.walPath = Objects.requireNonNull(walPath, "walPath");
        this.snapshotPath = Objects.requireNonNull(snapshotPath, "snapshotPath");
        if (compactionThresholdWrites < 1) {
            throw new IllegalArgumentException("compactionThresholdWrites must be positive");
        }
        this.compactionThresholdWrites = compactionThresholdWrites;
        Path parent = walPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        if (!Files.exists(walPath)) {
            Files.createFile(walPath);
        }
    }

    public synchronized void append(String key, VersionedValue value) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE)))) {
            output.writeInt(MAGIC);
            output.writeByte(VERSION);
            output.writeUTF(key);
            writeVersion(output, value);
            output.flush();
        }
        writesSinceCompaction++;
    }

    public synchronized Map<String, VersionedValue> replay() throws IOException {
        Map<String, VersionedValue> latest = new LinkedHashMap<>();
        restore().forEach((key, versions) ->
                latest.put(key, VersionedValue.latestIncludingTombstone(versions)));
        return latest;
    }

    public synchronized Map<String, List<VersionedValue>> restore() throws IOException {
        Map<String, List<VersionedValue>> restored = loadSnapshot();
        replayWalInto(restored);
        return restored;
    }

    public synchronized boolean shouldCompact() {
        return writesSinceCompaction >= compactionThresholdWrites;
    }

    public synchronized void compact(Map<String, List<VersionedValue>> snapshot) throws IOException {
        Objects.requireNonNull(snapshot, "snapshot");
        Path parent = snapshotPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tempSnapshot = snapshotPath.resolveSibling(snapshotPath.getFileName() + ".tmp");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                tempSnapshot,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)))) {
            output.writeInt(SNAPSHOT_MAGIC);
            output.writeByte(VERSION);
            output.writeInt(snapshot.size());
            for (Map.Entry<String, List<VersionedValue>> entry : snapshot.entrySet()) {
                output.writeUTF(entry.getKey());
                output.writeInt(entry.getValue().size());
                for (VersionedValue version : entry.getValue()) {
                    writeVersion(output, version);
                }
            }
        }
        Files.move(tempSnapshot, snapshotPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.newOutputStream(walPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING).close();
        writesSinceCompaction = 0;
    }

    public synchronized long sizeBytes() throws IOException {
        return Files.exists(walPath) ? Files.size(walPath) : 0L;
    }

    public Path walPath() {
        return walPath;
    }

    public Path snapshotPath() {
        return snapshotPath;
    }

    private Map<String, VersionedValue> replayWalLatestOnly() throws IOException {
        Map<String, VersionedValue> restored = new LinkedHashMap<>();
        if (Files.size(walPath) == 0) {
            return restored;
        }
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(walPath)))) {
            while (true) {
                try {
                    int magic = input.readInt();
                    if (magic != MAGIC) {
                        throw new IOException("invalid WAL record magic in " + walPath);
                    }
                    byte version = input.readByte();
                    if (version != VERSION) {
                        throw new IOException("unsupported WAL version " + version);
                    }
                    String key = input.readUTF();
                    restored.put(key, readVersion(input));
                } catch (EOFException ignored) {
                    break;
                }
            }
        }
        return restored;
    }

    private void replayWalInto(Map<String, List<VersionedValue>> restored) throws IOException {
        if (Files.size(walPath) == 0) {
            return;
        }
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(walPath)))) {
            while (true) {
                try {
                    int magic = input.readInt();
                    if (magic != MAGIC) {
                        throw new IOException("invalid WAL record magic in " + walPath);
                    }
                    byte version = input.readByte();
                    if (version != VERSION) {
                        throw new IOException("unsupported WAL version " + version);
                    }
                    String key = input.readUTF();
                    restored.merge(key, List.of(readVersion(input)), this::mergeVersions);
                } catch (EOFException ignored) {
                    break;
                }
            }
        }
    }

    private Map<String, List<VersionedValue>> loadSnapshot() throws IOException {
        Map<String, List<VersionedValue>> restored = new LinkedHashMap<>();
        if (!Files.exists(snapshotPath) || Files.size(snapshotPath) == 0) {
            return restored;
        }
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(snapshotPath)))) {
            int magic = input.readInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new IOException("invalid snapshot magic in " + snapshotPath);
            }
            byte version = input.readByte();
            if (version != VERSION) {
                throw new IOException("unsupported snapshot version " + version);
            }
            int keyCount = input.readInt();
            for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                String key = input.readUTF();
                int versionCount = input.readInt();
                List<VersionedValue> versions = new ArrayList<>(versionCount);
                for (int versionIndex = 0; versionIndex < versionCount; versionIndex++) {
                    versions.add(readVersion(input));
                }
                restored.put(key, List.copyOf(versions));
            }
        }
        return restored;
    }

    private List<VersionedValue> mergeVersions(List<VersionedValue> left, List<VersionedValue> right) {
        List<VersionedValue> merged = new ArrayList<>(left);
        merged.addAll(right);
        return List.copyOf(merged);
    }

    private void writeVersion(DataOutputStream output, VersionedValue value) throws IOException {
        output.writeLong(value.timestampEpochMs());
        output.writeBoolean(value.tombstone());
        byte[] bytes = value.value();
        output.writeInt(bytes.length);
        output.write(bytes);
        output.writeInt(value.vectorClock().size());
        for (Map.Entry<String, Long> entry : value.vectorClock().entrySet()) {
            output.writeUTF(entry.getKey());
            output.writeLong(entry.getValue());
        }
    }

    private VersionedValue readVersion(DataInputStream input) throws IOException {
        long timestamp = input.readLong();
        boolean tombstone = input.readBoolean();
        int valueLength = input.readInt();
        if (valueLength < 0) {
            throw new IOException("invalid value length " + valueLength);
        }
        byte[] value = input.readNBytes(valueLength);
        if (value.length != valueLength) {
            throw new EOFException("truncated value");
        }
        int vectorClockSize = input.readInt();
        Map<String, Long> vectorClock = new LinkedHashMap<>();
        for (int index = 0; index < vectorClockSize; index++) {
            vectorClock.put(input.readUTF(), input.readLong());
        }
        return new VersionedValue(value, timestamp, vectorClock, tombstone);
    }
}
