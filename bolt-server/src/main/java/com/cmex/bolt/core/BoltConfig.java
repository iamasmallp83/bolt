package com.cmex.bolt.core;

public record BoltConfig(
        int port, boolean isProd, int group, int sequencerSize, int matchingSize, int responseSize

) {
    public static final BoltConfig DEFAULT = new BoltConfig(9090, false, 4, 1024,
            512, 512);
}