package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 主节点服务器
 */
public class MasterServer {
    
    private final int port;
    private final ReplicationManager replicationManager;
    private final MasterReplicationServiceImpl masterService;
    private Server server;
    
    // 存储从节点的stub连接
    private final ConcurrentMap<Integer, SlaveReplicationServiceGrpc.SlaveReplicationServiceBlockingStub> slaveStubs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, SlaveReplicationServiceGrpc.SlaveReplicationServiceStub> slaveAsyncStubs = new ConcurrentHashMap<>();
    
    public MasterServer(int port, ReplicationManager replicationManager) {
        this.port = port;
        this.replicationManager = replicationManager;
        this.masterService = new MasterReplicationServiceImpl(replicationManager);
    }
    
    /**
     * 启动主节点服务器
     */
    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(masterService)
                .build()
                .start();
        
        System.out.println("Master server started, listening on port " + port);
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down master server");
            try {
                MasterServer.this.stop();
            } catch (InterruptedException e) {
                System.err.println("Error shutting down master server: " + e.getMessage());
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
            
            System.out.println("Created slave stub for node " + nodeId + " at " + slaveHost + ":" + slavePort);
            
        } catch (Exception e) {
            System.err.println("Failed to create slave stub for node " + nodeId + " at " + slaveHost + ":" + slavePort + ": " + e.getMessage());
        }
    }
    
    /**
     * 发送业务消息到从节点
     */
    public void sendBusinessToSlave(int nodeId, BatchBusinessMessage businessMessage) {
        try {
            SlaveReplicationServiceGrpc.SlaveReplicationServiceStub slaveStub = slaveAsyncStubs.get(nodeId);
            if (slaveStub != null) {
                StreamObserver<BatchBusinessMessage> requestObserver = slaveStub.sendBusiness(new StreamObserver<ConfirmationMessage>() {
                    @Override
                    public void onNext(ConfirmationMessage confirmation) {
                        System.out.println("Slave " + nodeId + " confirmed business message sequence " + confirmation.getSequence());
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error sending business message to slave " + nodeId + ": " + t.getMessage());
                    }
                    
                    @Override
                    public void onCompleted() {
                        System.out.println("Business message stream to slave " + nodeId + " completed.");
                    }
                });
                
                requestObserver.onNext(businessMessage);
                requestObserver.onCompleted();
            } else {
                System.err.println("No stub found for slave " + nodeId + ". Cannot send business message.");
            }
            
        } catch (Exception e) {
            System.err.println("Failed to send business message to slave node " + nodeId + ": " + e.getMessage());
        }
    }
    
    /**
     * 发送快照数据到从节点
     */
    public void sendSnapshotToSlave(int nodeId, SnapshotDataMessage snapshotMessage) {
        try {
            SlaveReplicationServiceGrpc.SlaveReplicationServiceStub slaveStub = slaveAsyncStubs.get(nodeId);
            if (slaveStub != null) {
                StreamObserver<SnapshotDataMessage> requestObserver = slaveStub.sendSnapshot(new StreamObserver<ConfirmationMessage>() {
                    @Override
                    public void onNext(ConfirmationMessage confirmation) {
                        System.out.println("Slave " + nodeId + " confirmed snapshot timestamp " + confirmation.getSequence());
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error sending snapshot to slave " + nodeId + ": " + t.getMessage());
                    }
                    
                    @Override
                    public void onCompleted() {
                        System.out.println("Snapshot stream to slave " + nodeId + " completed.");
                    }
                });
                
                requestObserver.onNext(snapshotMessage);
                requestObserver.onCompleted();
            } else {
                System.err.println("No stub found for slave " + nodeId + ". Cannot send snapshot.");
            }
            
        } catch (Exception e) {
            System.err.println("Failed to send snapshot to slave node " + nodeId + ": " + e.getMessage());
        }
    }
    
    /**
     * 发送Journal重放到从节点
     */
    public void sendJournalToSlave(int nodeId, JournalReplayMessage journalMessage) {
        try {
            SlaveReplicationServiceGrpc.SlaveReplicationServiceStub slaveStub = slaveAsyncStubs.get(nodeId);
            if (slaveStub != null) {
                StreamObserver<JournalReplayMessage> requestObserver = slaveStub.sendJournal(new StreamObserver<ConfirmationMessage>() {
                    @Override
                    public void onNext(ConfirmationMessage confirmation) {
                        System.out.println("Slave " + nodeId + " confirmed journal sequence " + confirmation.getSequence());
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Error sending journal to slave " + nodeId + ": " + t.getMessage());
                    }
                    
                    @Override
                    public void onCompleted() {
                        System.out.println("Journal stream to slave " + nodeId + " completed.");
                    }
                });
                
                requestObserver.onNext(journalMessage);
                requestObserver.onCompleted();
            } else {
                System.err.println("No stub found for slave " + nodeId + ". Cannot send journal.");
            }
            
        } catch (Exception e) {
            System.err.println("Failed to send journal to slave node " + nodeId + ": " + e.getMessage());
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
