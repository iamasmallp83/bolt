package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.handler.TcpReplicationClient;
import lombok.extern.slf4j.Slf4j;

import java.lang.InterruptedException;

/**
 * Bolt从节点 - 负责连接到主节点并接收复制请求
 * 新架构：JournalHandler -> AckHandler -> SequencerDispatcher
 * SequencerDisruptor事件来源：TcpReplicationClient
 */
@Slf4j
public class BoltSlave extends BoltBase {

    private final TcpReplicationClient replicationClient;

    public BoltSlave(BoltConfig config) {
        super(config);

        // 验证从节点配置
        if (config.isMaster()) {
            throw new IllegalArgumentException("BoltSlave requires slave configuration");
        }
        
        // 创建TcpReplicationClient
        this.replicationClient = new TcpReplicationClient(
            config.masterHost(),
            config.masterPort(),
            config,
            "slave-" + config.replicationPort(),
            envoyServer.getSequencerRingBuffer()
        );
        
        // 设置TcpReplicationClient引用到EnvoyServer
        envoyServer.setTcpReplicationClient(replicationClient);
        
        log.info("BoltSlave initialized with EnvoyServer and TcpReplicationClient");
    }

    @Override
    protected void startNodeSpecificServices() {
        try {
            // 连接到主节点
            replicationClient.connect();
            log.info("Slave replication client connected successfully to master at {}:{}", 
                    config.masterHost(), config.masterPort());
        } catch (Exception e) {
            log.error("Failed to connect to master at {}:{}", 
                    config.masterHost(), config.masterPort(), e);
            throw new RuntimeException("Failed to connect to master", e);
        }
    }

    @Override
    protected void stopNodeSpecificServices() {
        log.info("Stopping slave replication client");
        try {
            replicationClient.disconnect();
            log.info("Slave replication client disconnected successfully");
        } catch (Exception e) {
            log.error("Failed to disconnect slave replication client", e);
        }
    }

    protected void logNodeSpecificInfo() {
        log.info("=== BoltSlave Node Information ===");
        log.info("Master Host: {}", config.masterHost());
        log.info("Master Port: {}", config.masterPort());
        log.info("Replication Port: {}", config.replicationPort());
        log.info("Replication Client Status: {}", 
                replicationClient != null && replicationClient.isConnected() ? "Connected" : "Disconnected");
        log.info("=====================================");
    }

    /**
     * 获取复制客户端
     */
    public TcpReplicationClient getReplicationClient() {
        return replicationClient;
    }
    
    /**
     * 检查是否已连接到主节点
     */
    public boolean isConnectedToMaster() {
        return replicationClient != null && replicationClient.isConnected();
    }

    /**
     * 从节点入口点
     */
    public static void main(String[] args) {
        try {
            BoltConfig config = BoltConfig.DEFAULT; // 使用默认配置
            BoltSlave slave = new BoltSlave(config);
            slave.start();
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down BoltSlave...");
                // BoltBase已经处理了关闭逻辑
            }));
            
            // 保持运行
            slave.awaitTermination();
            
        } catch (Exception e) {
            log.error("Failed to start BoltSlave", e);
            System.exit(1);
        }
    }
    
    /**
     * 等待终止
     */
    private void awaitTermination() throws InterruptedException {
        if (nettyServer != null) {
            nettyServer.awaitTermination();
        }
    }
}
