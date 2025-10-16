package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.ReplicationClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Bolt从节点 - 使用组合模式和依赖注入解耦
 * 负责启动从节点特有的服务：SlaveServer + SlaveSyncManager
 */
@Getter
@Slf4j
public class BoltSlave {
    // Getters
    private final BoltConfig config;
    private BoltCore core;

    public BoltSlave(BoltConfig config) {
        // 验证从节点配置
        if (config.isMaster()) {
            throw new IllegalArgumentException("BoltSlave requires slave configuration");
        }
        this.config = config;
        log.info("BoltSlave initialized with ReplicationContext");
    }

    /**
     * 启动从节点
     */
    public void start() throws IOException, InterruptedException {
        log.info("Starting BoltSlave");

        // 1. 启动从节点特有服务（需要RingBuffer）
        startSlaveSpecificServices();

        // 2. 先启动基础组件（EnvoyServer + gRPC + 监控）
        this.core = new BoltCore(config);
        // 将RingBuffer注入到ReplicationContext
        core.start();


        log.info("BoltSlave started successfully");

        // 3. 等待关闭
        awaitShutdown();
    }

    /**
     * 启动从节点特有服务
     */
    private void startSlaveSpecificServices() throws IOException {
        log.info("Starting slave-specific services");

        // 启动从节点复制服务（SlaveServer）
        ReplicationClient replicationClient = new ReplicationClient(config.nodeId(), config.masterHost(), config.masterReplicationPort(), config);
        replicationClient.start();


        log.info("Slave-specific services started successfully");
    }

    /**
     * 停止从节点
     */
    public void stop() {
        log.info("Stopping BoltSlave");

        // 停止从节点特有服务
        stopSlaveSpecificServices();

        // 停止基础组件
        core.stop();

        log.info("BoltSlave stopped");
    }

    /**
     * 停止从节点特有服务
     */
    private void stopSlaveSpecificServices() {
        log.info("Stopping slave-specific services");

        // 停止从节点复制服务
//        if (slaveServer != null) {
//            try {
//                slaveServer.stop();
//            } catch (InterruptedException e) {
//                log.error("Error stopping slave server", e);
//                Thread.currentThread().interrupt();
//            }
//        }

        log.info("Slave-specific services stopped");
    }

    /**
     * 等待关闭
     */
    private void awaitShutdown() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                log.error("Error during BoltSlave shutdown: {}", e.getMessage(), e);
            }
        }));

        core.awaitShutdown();
    }

}
