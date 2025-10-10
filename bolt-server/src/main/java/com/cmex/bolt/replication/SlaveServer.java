package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 从节点服务器
 */
@Slf4j
public class SlaveServer {
    
    private final int port;
    private final SlaveReplicationManager slaveReplicationManager;
    private final SlaveReplicationServiceImpl slaveService;
    private Server server;
    
    public SlaveServer(int port, SlaveReplicationManager slaveReplicationManager) {
        this.port = port;
        this.slaveReplicationManager = slaveReplicationManager;
        this.slaveService = new SlaveReplicationServiceImpl(slaveReplicationManager);
    }
    
    /**
     * 启动从节点服务器
     */
    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(slaveService)
                .build()
                .start();
        
        log.info("Slave server started, listening on port {}", port);
        
        // 启动从节点复制管理器
        slaveReplicationManager.start();
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down slave server");
            try {
                SlaveServer.this.stop();
            } catch (InterruptedException e) {
                log.error("Error shutting down slave server", e);
                Thread.currentThread().interrupt();
            }
        }));
    }
    
    /**
     * 停止从节点服务器
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
        
        // 停止从节点复制管理器
        slaveReplicationManager.stop();
    }
    
    /**
     * 等待服务器终止
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    
    /**
     * 获取从节点复制管理器
     */
    public SlaveReplicationManager getSlaveReplicationManager() {
        return slaveReplicationManager;
    }
    
    /**
     * 获取从节点服务实现
     */
    public SlaveReplicationServiceImpl getSlaveService() {
        return slaveService;
    }
}
