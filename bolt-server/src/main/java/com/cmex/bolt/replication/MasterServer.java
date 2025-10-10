package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 主节点服务器
 */
@Slf4j
public class MasterServer {
    
    private final BoltConfig config;
    private final ReplicationManager replicationManager;
    private final MasterReplicationServiceImpl masterService;
    private Server server;
    
    // 存储从节点的stub连接
    private final ConcurrentMap<Integer, SlaveReplicationServiceGrpc.SlaveReplicationServiceBlockingStub> slaveStubs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, SlaveReplicationServiceGrpc.SlaveReplicationServiceStub> slaveAsyncStubs = new ConcurrentHashMap<>();
    
    public MasterServer(BoltConfig boltConfig, ReplicationManager replicationManager) {
        this.config = boltConfig;
        this.replicationManager = replicationManager;
        this.masterService = new MasterReplicationServiceImpl(replicationManager);
    }
    
    /**
     * 启动主节点服务器
     */
    public void start() throws IOException {
        server = ServerBuilder.forPort(config.masterReplicationPort())
                .addService(masterService)
                .build()
                .start();

        log.info("Replication Master server started, listening on port {}", config.masterReplicationPort());
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down master server");
            try {
                MasterServer.this.stop();
            } catch (InterruptedException e) {
                log.info("Error shutting down master server: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
        }));
    }
    
    /**
     * 停止主节点服务器
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
        
        // 关闭所有从节点连接
        slaveStubs.clear();
        slaveAsyncStubs.clear();
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
     * 创建从节点的stub连接
     */
    public void createSlaveStub(int nodeId, String slaveHost, int slavePort) {
        try {
            // 创建到从节点的连接
            io.grpc.ManagedChannel slaveChannel = io.grpc.ManagedChannelBuilder
                    .forAddress(slaveHost, slavePort)
                    .usePlaintext()
                    .build();
            
            // 创建stub
            SlaveReplicationServiceGrpc.SlaveReplicationServiceBlockingStub slaveStub = 
                    SlaveReplicationServiceGrpc.newBlockingStub(slaveChannel);
            SlaveReplicationServiceGrpc.SlaveReplicationServiceStub slaveAsyncStub = 
                    SlaveReplicationServiceGrpc.newStub(slaveChannel);
            
            // 存储stub
            slaveStubs.put(nodeId, slaveStub);
            slaveAsyncStubs.put(nodeId, slaveAsyncStub);

            log.info("Created slave stub for node {} at {}:{}", nodeId, slaveHost, slavePort);
            
        } catch (Exception e) {
            log.error("Failed to create slave stub for node {} at {}:{}: {}", nodeId, slaveHost, slavePort, e.getMessage());
        }
    }
    
    /**
     * 获取从节点stub
     */
    public SlaveReplicationServiceGrpc.SlaveReplicationServiceBlockingStub getSlaveStub(int nodeId) {
        return slaveStubs.get(nodeId);
    }
    
    /**
     * 获取从节点异步stub
     */
    public SlaveReplicationServiceGrpc.SlaveReplicationServiceStub getSlaveAsyncStub(int nodeId) {
        return slaveAsyncStubs.get(nodeId);
    }
    
    /**
     * 移除从节点stub
     */
    public void removeSlaveStub(int nodeId) {
        slaveStubs.remove(nodeId);
        slaveAsyncStubs.remove(nodeId);
        System.out.println("Removed slave stub for node " + nodeId);
    }
    
    /**
     * 获取所有从节点ID
     */
    public java.util.Set<Integer> getAllSlaveNodeIds() {
        return slaveStubs.keySet();
    }
}
