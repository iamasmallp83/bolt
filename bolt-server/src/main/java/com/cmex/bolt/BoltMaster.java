package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.handler.TcpReplicationServer;
import lombok.extern.slf4j.Slf4j;

import java.lang.InterruptedException;

/**
 * Bolt主节点 - 负责处理复制服务
 */
@Slf4j
public class BoltMaster extends BoltBase {

    private TcpReplicationServer replicationServer;

    public BoltMaster(BoltConfig config) {
        super(config);

        // 验证主节点配置
        if (!config.isMaster()) {
            throw new IllegalArgumentException("BoltMaster requires isMaster=true in config");
        }

    }

    @Override
    protected void startNodeSpecificServices() {
        // 启动复制服务（如果启用）
        log.info("Starting replication server on port {}", config.replicationPort());
        try {
            this.replicationServer = new TcpReplicationServer(
                config.replicationPort(), 
                config, 
                envoyServer.getBarrierHandler(),
                envoyServer.getReplicationHandler()
            );
            replicationServer.start();
            
            // 设置TcpReplicationServer引用到ReplicationHandler
            envoyServer.getReplicationHandler().setTcpReplicationServer(replicationServer);
        } catch (InterruptedException e) {
            log.error("Failed to start replication server on port {} - InterruptedException",
                    config.replicationPort(), e);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start replication server", e);
        } catch (Exception e) {
            log.error("Failed to start replication server on port {}", config.replicationPort(), e);
            throw new RuntimeException("Failed to start replication server", e);
        }
    }

    @Override
    protected void stopNodeSpecificServices() {
        if (replicationServer != null) {
            log.info("Stopping replication server...");
            replicationServer.stop();
            log.info("Replication server stopped");
        }
    }

    @Override
    public boolean isRunning() {
        boolean baseRunning = super.isRunning();
        boolean replicationRunning = replicationServer == null || replicationServer.isRunning();
        return baseRunning && replicationRunning;
    }

    /**
     * 获取复制服务器
     */
    public TcpReplicationServer getReplicationServer() {
        return replicationServer;
    }

    /**
     * 检查复制服务是否运行
     */
    public boolean isReplicationRunning() {
        return replicationServer != null && replicationServer.isRunning();
    }

    /**
     * 获取活跃从节点数量
     */
    public int getActiveSlaveCount() {
        return replicationServer != null ? replicationServer.getActiveSlaveCount() : 0;
    }
}

