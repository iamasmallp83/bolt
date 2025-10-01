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
        int batchSize,
        // 毫秒
        int batchTimeout,
        // 日志配置
        boolean enableJournal,
        // 目录配置
        String boltHome
) {
    public static final BoltConfig DEFAULT = new BoltConfig(9090, false, 4, 1024,
            512, 512, true, 9091, "journal", false, true, "localhost", 9090, 9092, 100, 5000, false, ".");

    public String journalFilePath() {
        return boltHome + "/journal/" + journalFilePath + (isBinary ? ".data" : ".json");
    }
    

    public String journalDir() {
        return boltHome + "/journal";
    }

    public static BoltConfig parseConfig(String[] args) {
        if (args.length == 0) {
            return BoltConfig.DEFAULT;
        } else if (args.length == 19) {
            try {
                int port = Integer.parseInt(args[0]);
                boolean isProd = Boolean.parseBoolean(args[1]);
                int group = Integer.parseInt(args[2]);
                int sequencerSize = Integer.parseInt(args[3]);
                int matchingSize = Integer.parseInt(args[4]);
                int responseSize = Integer.parseInt(args[5]);
                boolean enablePrometheus = Boolean.parseBoolean(args[6]);
                int prometheusPort = Integer.parseInt(args[7]);
                String journalFilePath = args[8];
                boolean isBinary = Boolean.parseBoolean(args[9]);
                boolean isMaster = Boolean.parseBoolean(args[10]);
                String masterHost = args[11];
                int masterPort = Integer.parseInt(args[12]);
                int replicationPort = Integer.parseInt(args[13]);
                int batchSize = Integer.parseInt(args[15]);
                int batchTimeout = Integer.parseInt(args[16]);
                boolean enableJournal = Boolean.parseBoolean(args[17]);
                String boltHome = args[18];

                return new BoltConfig(port, isProd, group, sequencerSize, matchingSize, responseSize,
                        enablePrometheus, prometheusPort, journalFilePath, isBinary, isMaster, masterHost,
                        masterPort, replicationPort,  batchSize, batchTimeout, enableJournal, boltHome);
            } catch (NumberFormatException e) {
                printUsage();
                throw new IllegalArgumentException("Invalid command line arguments", e);
            }
        } else {
            printUsage();
            throw new IllegalArgumentException("Invalid number of arguments: " + args.length);
        }
    }

    /**
     * 打印使用说明
     */
    public static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar bolt.jar  # 使用默认配置");
        System.out.println("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary}");
        System.out.println("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary} {isMaster} {masterHost} {masterPort} {replicationPort} {enableReplication} {batchSize} {batchTimeoutMs} {enableJournal} {boltHome}");
        System.out.println("Logback Configuration:");
        System.out.println("  - Master mode: uses master-logback.xml (creates bolt-master.log)");
        System.out.println("  - Slave mode: uses slave-logback.xml (creates bolt-slave.log)");
        System.out.println("  - Override logs directory: -DLOGS_DIR=/path/to/logs");
        System.out.println("Examples:");
        System.out.println("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false");
        System.out.println("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false false localhost 9090 9092 true 100 5000 true /path/to/bolt");
        System.out.println("  java -DLOGS_DIR=/var/log/bolt -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false true localhost 9090 9092 true 100 5000 true /path/to/bolt");
    }
}