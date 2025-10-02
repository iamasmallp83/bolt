package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.handler.TcpReplicationServer;
import lombok.extern.slf4j.Slf4j;

import java.lang.InterruptedException;

/**
 * Bolt主节点 - 负责处理复制服务
 * 新架构：JournalHandler -> ReplicationHandler -> ConfirmHandler -> SequencerDispatcher
 */
@Slf4j
public class BoltMaster extends BoltBase {

    private final TcpReplicationServer replicationServer;

    public BoltMaster(BoltConfig config) {
        super(config);

        // 验证主节点配置
        if (!config.isMaster()) {
            throw new IllegalArgumentException("BoltMaster requires master configuration");
        }
        
        // 创建TcpReplicationServer
        this.replicationServer = new TcpReplicationServer(config.replicationPort(), 
            envoyServer.getReplicationState());
        
        // 设置TcpReplicationServer引用到ReplicationHandler
        if (envoyServer.getReplicationHandler() != null) {
            envoyServer.getReplicationHandler().setTcpReplicationServer(replicationServer);
        }
        
        // ConfirmHandler已移除，不再需要设置引用
        
        log.info("BoltMaster initialized with EnvoyServer and TcpReplicationServer");
    }

    @Override
    protected void startNodeSpecificServices() {
        try {
            // 启动复制服务器
            replicationServer.start();
            log.info("Master replication server started successfully on port {}", config.replicationPort());
        } catch (Exception e) {
            log.error("Failed to start master replication server", e);
            throw new RuntimeException("Failed to start master replication server", e);
        }
    }

    @Override
    protected void stopNodeSpecificServices() {
        log.info("Stopping master replication server");
        try {
            replicationServer.stop();
            log.info("Master replication server stopped successfully");
        } catch (Exception e) {
            log.error("Failed to stop master replication server", e);
        }
    }

    protected void logNodeSpecificInfo() {
        log.info("=== BoltMaster Node Information ===");
        log.info("Master Host: {}", config.masterHost());
        log.info("Master Port: {}", config.masterPort());
        log.info("Replication Port: {}", config.replicationPort());
        log.info("Replication Server Status: {}", replicationServer != null ? "Running" : "Not Started");
        log.info("=====================================");
    }

    /**
     * 获取复制服务器
     */
    public TcpReplicationServer getReplicationServer() {
        return replicationServer;
    }
    
    /**
     * 检查复制服务器是否运行
     */
    public boolean isReplicationServerRunning() {
        return replicationServer != null;
    }

    /**
     * 主节点入口点
     */
    public static void main(String[] args) {
        try {
            BoltConfig config = BoltConfig.DEFAULT; // 使用默认配置
            BoltMaster master = new BoltMaster(config);
            master.start();
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down BoltMaster...");
                // BoltBase已经处理了关闭逻辑
            }));
            
            // 保持运行
            master.awaitTermination();
            
        } catch (Exception e) {
            log.error("Failed to start BoltMaster", e);
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
