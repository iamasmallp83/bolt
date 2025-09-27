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
        // 主节点相关配置
        boolean isMaster,
        String masterHost,
        int masterPort,
        int replicationPort,
        boolean enableReplication,
        int batchSize,
        int batchTimeoutMs,
        // 日志配置
        boolean enableJournal,
        // 目录配置
        String boltHome
) {
    public static final BoltConfig DEFAULT = new BoltConfig(9090, false, 4, 1024,
            512, 512, true, 9091, "journal", false, true, "localhost", 9090, 9092, false, 100, 5000, false, ".");

    public String journalFilePath() {
        return boltHome + "/journal/" + journalFilePath + (isBinary ? ".data" : ".json");
    }
    

    public String journalDir() {
        return boltHome + "/journal";
    }
}