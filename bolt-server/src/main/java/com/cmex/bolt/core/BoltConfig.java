package com.cmex.bolt.core;

public record BoltConfig(
        int nodeId,
        String boltHome,
        int port,
        boolean isProd,
        int group,
        int sequencerSize,
        int matchingSize,
        int responseSize,
        //监控
        boolean enablePrometheus,
        int prometheusPort,
        // 主从点相关配置
        boolean isMaster,
        String masterHost,
        int masterPort,
        int replicationPort,
        int batchSize,
        int batchTimeout,
        // 日志配置
        boolean enableJournal,
        String journalFilePath,
        boolean isBinary,
        // Snapshot配置
        int snapshotInterval
) {
    public static final BoltConfig DEFAULT = new BoltConfig(1, ".", 9090, false, 4, 1024,
            512, 512, true, 9091, true,
            "localhost", 9090, 9092, 100, 5000,
            false, "journal", false, 60);

    public String journalFilePath() {
        return boltHome + "/journal/" + journalFilePath + (isBinary ? ".data" : ".json");
    }
    

    public String journalDir() {
        return boltHome + "/journal";
    }

    public static BoltConfig parseConfig(String[] args) {
        if (args.length == 0) {
            return BoltConfig.DEFAULT;
        } else if (args.length == 21) {
            try {
                int nodeId = Integer.parseInt(args[0]);
                String boltHome = args[1];
                int port = Integer.parseInt(args[2]);
                boolean isProd = Boolean.parseBoolean(args[3]);
                int group = Integer.parseInt(args[4]);
                int sequencerSize = Integer.parseInt(args[5]);
                int matchingSize = Integer.parseInt(args[6]);
                int responseSize = Integer.parseInt(args[7]);
                boolean enablePrometheus = Boolean.parseBoolean(args[8]);
                int prometheusPort = Integer.parseInt(args[9]);
                boolean isMaster = Boolean.parseBoolean(args[10]);
                String masterHost = args[11];
                int masterPort = Integer.parseInt(args[12]);
                int replicationPort = Integer.parseInt(args[13]);
                int batchSize = Integer.parseInt(args[14]);
                int batchTimeout = Integer.parseInt(args[15]);
                boolean enableJournal = Boolean.parseBoolean(args[16]);
                String journalFilePath = args[17];
                boolean isBinary = Boolean.parseBoolean(args[18]);
                int snapshotInterval = Integer.parseInt(args[19]);

                return new BoltConfig(nodeId, boltHome, port, isProd, group, sequencerSize, matchingSize, responseSize,
                        enablePrometheus, prometheusPort, isMaster, masterHost, masterPort, replicationPort,
                        batchSize, batchTimeout, enableJournal, journalFilePath, isBinary, snapshotInterval);
            } catch (NumberFormatException e) {
                printUsage();
                throw new IllegalArgumentException("Invalid command line arguments", e);
            }
        } else {
            printUsage();
            throw new IllegalArgumentException("Invalid number of arguments: " + args.length);
        }
    }

    public boolean isSlave(){
        return !isMaster();
    }

    /**
     * 打印使用说明
     */
    public static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar bolt.jar  # 使用默认配置");
        System.out.println("  java -jar bolt.jar {nodeId} {boltHome} {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {enablePrometheus} {prometheusPort} {isMaster} {masterHost} {masterPort} {replicationPort} {batchSize} {batchTimeoutMs} {enableJournal} {journalFilePath} {isBinary} {snapshotIntervalSeconds}");
        System.out.println("Logback Configuration:");
        System.out.println("  - Master mode: uses master-logback.xml (creates bolt-master.log)");
        System.out.println("  - Slave mode: uses slave-logback.xml (creates bolt-slave.log)");
        System.out.println("  - Override logs directory: -DLOGS_DIR=/path/to/logs");
        System.out.println("Examples:");
        System.out.println("  java -jar bolt.jar 1 /path/to/bolt 9090 true 4 1024 512 512 true 9091 true localhost 9090 9092 100 5000 false journal false 300");
        System.out.println("  java -DLOGS_DIR=/var/log/bolt -jar bolt.jar 1 /path/to/bolt 9090 true 4 1024 512 512 true 9091 false localhost 9090 9092 100 5000 true journal.data true 600");
    }
}