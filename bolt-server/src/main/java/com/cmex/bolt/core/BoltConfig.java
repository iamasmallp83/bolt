package com.cmex.bolt.core;

public record BoltConfig(
        int port,
        boolean isProd,
        int group,
        int sequencerSize,
        int matchingSize,
        int responseSize,
        boolean enablePrometheus,
        int prometheusPort,
        String journalFilePath,
        boolean isBinary,
        // 从节点相关配置
        boolean isSlave,
        String masterHost,
        int masterPort,
        int replicationPort,
        boolean enableReplication,
        int batchSize,
        int batchTimeoutMs,
        // 测试模式配置
        boolean isTest
) {
    public static final BoltConfig DEFAULT = new BoltConfig(9090, false, 4, 1024,
            512, 512, true, 9091, "journal", false, false, "localhost", 9090, 9092, false, 100, 5000, false);

    public String journalFilePath() {
        return journalFilePath + (isBinary ? ".data" : ".json");
    }
}