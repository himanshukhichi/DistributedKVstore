package com.distkv.model;

import java.util.Objects;

public record NodeEndpoint(String nodeId, String host, int grpcPort) {
    public NodeEndpoint {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(host, "host");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (grpcPort < 1 || grpcPort > 65_535) {
            throw new IllegalArgumentException("grpcPort must be between 1 and 65535");
        }
    }
}
