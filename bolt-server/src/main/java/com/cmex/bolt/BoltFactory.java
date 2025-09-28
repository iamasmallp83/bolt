package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Bolt工厂类 - 根据配置创建相应的Bolt实例
 */
@Slf4j
public class BoltFactory {
    
    /**
     * 根据配置创建Bolt实例
     */
    public static BoltBase createBolt(BoltConfig config) {
        if (config.isMaster()) {
            log.info("Creating BoltMaster instance");
            return new BoltMaster(config);
        } else {
            log.info("Creating BoltSlave instance");
            return new BoltSlave(config);
        }
    }
    
    /**
     * 解析命令行参数并创建Bolt实例
     */
    public static BoltBase createBoltFromArgs(String[] args) {
        BoltConfig config = parseConfig(args);
        return createBolt(config);
    }
    
    /**
     * 解析命令行配置
     */
    private static BoltConfig parseConfig(String[] args) {
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
                boolean enableReplication = Boolean.parseBoolean(args[14]);
                int batchSize = Integer.parseInt(args[15]);
                int batchTimeoutMs = Integer.parseInt(args[16]);
                boolean enableJournal = Boolean.parseBoolean(args[17]);
                String boltHome = args[18];
                
                return new BoltConfig(port, isProd, group, sequencerSize, matchingSize, responseSize, 
                    enablePrometheus, prometheusPort, journalFilePath, isBinary, isMaster, masterHost, 
                    masterPort, replicationPort, enableReplication, batchSize, batchTimeoutMs, enableJournal, boltHome);
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
    private static void printUsage() {
        log.info("Usage:");
        log.info("  java -jar bolt.jar  # 使用默认配置");
        log.info("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary}");
        log.info("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary} {isMaster} {masterHost} {masterPort} {replicationPort} {enableReplication} {batchSize} {batchTimeoutMs} {enableJournal} {boltHome}");
        log.info("Logback Configuration:");
        log.info("  - Master mode: uses master-logback.xml (creates bolt-master.log)");
        log.info("  - Slave mode: uses slave-logback.xml (creates bolt-slave.log)");
        log.info("  - Override logs directory: -DLOGS_DIR=/path/to/logs");
        log.info("Examples:");
        log.info("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false");
        log.info("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false false localhost 9090 9092 true 100 5000 true /path/to/bolt");
        log.info("  java -DLOGS_DIR=/var/log/bolt -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false true localhost 9090 9092 true 100 5000 true /path/to/bolt");
    }
}
