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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class WALManager {
    private static final int MAGIC = 0x44565741;
    private static final byte VERSION = 1;

    private final Path walPath;

    public WALManager(Path walPath) throws IOException {
        this.walPath = Objects.requireNonNull(walPath, "walPath");
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
            output.flush();
        }
    }

    public synchronized Map<String, VersionedValue> replay() throws IOException {
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
                    long timestamp = input.readLong();
                    boolean tombstone = input.readBoolean();
                    int valueLength = input.readInt();
                    if (valueLength < 0) {
                        throw new IOException("invalid WAL value length " + valueLength);
                    }
                    byte[] value = input.readNBytes(valueLength);
                    if (value.length != valueLength) {
                        break;
                    }
                    int vectorClockSize = input.readInt();
                    Map<String, Long> vectorClock = new LinkedHashMap<>();
                    for (int index = 0; index < vectorClockSize; index++) {
                        vectorClock.put(input.readUTF(), input.readLong());
                    }
                    restored.put(key, new VersionedValue(value, timestamp, vectorClock, tombstone));
                } catch (EOFException ignored) {
                    break;
                }
            }
        }
        return restored;
    }

    public Path walPath() {
        return walPath;
    }
}
