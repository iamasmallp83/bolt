package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.handler.SnapshotTrigger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Bolt主节点 - 使用组合模式
 * 负责启动主节点特有的服务：MasterServer + SnapshotTrigger
 */
@Getter
@Slf4j
public class BoltMaster {

    // Getters
    private final BoltCore core;
    private SnapshotTrigger snapshotTrigger;
    
    public BoltMaster(BoltConfig config) {
        // 验证主节点配置
        if (!config.isMaster()) {
            throw new IllegalArgumentException("BoltMaster requires master configuration");
        }
        
        this.core = new BoltCore(config);
        log.info("BoltMaster initialized");
    }
    
    /**
     * 启动主节点
     */
    public void start() throws IOException, InterruptedException {
        log.info("Starting BoltMaster");
        
        // 1. 启动基础组件（EnvoyServer + gRPC + 监控）
        core.start();
        
        // 2. 启动主节点特有服务
        startMasterSpecificServices();
        
        log.info("BoltMaster started successfully");
        
        // 3. 等待关闭
        awaitShutdown();
    }
    
    /**
     * 启动主节点特有服务
     */
    private void startMasterSpecificServices() throws IOException{
        log.info("Starting master-specific services");
        
        // 启动Snapshot触发器
        this.snapshotTrigger = new SnapshotTrigger(
            core.getConfig(),
            core.getEnvoyServer().getSequencerRingBuffer()
        );
        
        log.info("Master-specific services started successfully");
    }
    
    /**
     * 停止主节点
     */
    public void stop() {
        log.info("Stopping BoltMaster");
        
        // 停止主节点特有服务
        stopMasterSpecificServices();
        
        // 停止基础组件
        core.stop();
        
        log.info("BoltMaster stopped");
    }
    
    /**
     * 停止主节点特有服务
     */
    private void stopMasterSpecificServices() {
        log.info("Stopping master-specific services");
        
        // 停止Snapshot触发器
        if (snapshotTrigger != null) {
            snapshotTrigger.shutdown();
        }
        
    }

    /**
     * 等待关闭
     */
    private void awaitShutdown() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                log.error("Error during BoltMaster shutdown: {}", e.getMessage(), e);
            }
        }));
        
        core.awaitShutdown();
    }

}
