package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 复制管理器，管理所有slave节点的复制状态和服务
 */
@Slf4j
public class ReplicationManager {
    
    // 存储所有slave节点的信息
    private final ConcurrentMap<Integer, ReplicationInfo> slaveNodes = new ConcurrentHashMap<>();
    
    // MasterServer引用
    private MasterServer masterServer;
    
    // 定时任务执行器
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // 业务消息流观察者缓存
    private final ConcurrentMap<Integer, StreamObserver<BatchBusinessMessage>> businessStreamObservers = new ConcurrentHashMap<>();
    
    public ReplicationManager(MasterServer masterServer) {
        initializeServices();
        this.masterServer = masterServer;
    }
    
    /**
     * 初始化gRPC服务
     */
    private void initializeServices() {
        log.info("Initializing ReplicationManager services");
        // TODO: 根据实际配置初始化ReplicationService
        // 这里需要根据实际的gRPC服务器配置来设置
    }
    
    /**
     * 启动复制管理器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting ReplicationManager");
            
            // 启动心跳检查任务
//            scheduler.scheduleWithFixedDelay(this::checkHeartbeats, 30, 30, TimeUnit.SECONDS);
            
            // 启动状态同步任务
//            scheduler.scheduleWithFixedDelay(this::syncStates, 10, 10, TimeUnit.SECONDS);

            log.info("ReplicationManager started successfully");
        }
    }
    
    /**
     * 停止复制管理器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            System.out.println("Stopping ReplicationManager");
            
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            // 关闭所有连接
            slaveNodes.values().forEach(this::disconnectNode);
            
            System.out.println("ReplicationManager stopped");
        }
    }
    
    /**
     * 注册节点（双向）
     */
    public boolean registerNode(RegisterMessage registerMessage) {
        try {
            int nodeId = registerMessage.getNodeId();
            String host = registerMessage.getHost();
            int port = registerMessage.getPort();
            int replicationPort = registerMessage.getReplicationPort();
            
            ReplicationInfo replicationInfo = new ReplicationInfo(nodeId, host, port, replicationPort);
            replicationInfo.setState(ReplicationState.REGISTERED);
            replicationInfo.setConnected(true);
            
            slaveNodes.put(nodeId, replicationInfo);
            
            log.info("Node {} registered successfully: {}:{}", nodeId, host, port);
            
            // 延迟创建gRPC连接，给从节点时间启动复制服务
            scheduler.schedule(() -> {
                try {
                    createNodeConnection(replicationInfo);
                } catch (Exception e) {
                    log.error("Failed to create delayed connection to node {}", nodeId, e);
                }
            }, 2, TimeUnit.SECONDS);
            
            // 创建从节点的stub连接
            if (masterServer != null) {
                masterServer.createSlaveStub(nodeId, host, replicationPort);
            }
            
            return true;
        } catch (Exception e) {
            log.error("Failed to register node", e);
            return false;
        }
    }
    
    /**
     * 创建节点的gRPC连接
     */
    private void createNodeConnection(ReplicationInfo replicationInfo) {
        try {
            log.info("Attempting to create gRPC connection to slave node {} at {}:{}", 
                    replicationInfo.getNodeId(), replicationInfo.getHost(), replicationInfo.getReplicationPort());
            
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(replicationInfo.getHost(), replicationInfo.getReplicationPort())
                    .usePlaintext()
                    .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(5, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();
            
            // 创建异步stub
            SlaveReplicationServiceGrpc.SlaveReplicationServiceStub asyncStub = 
                    SlaveReplicationServiceGrpc.newStub(channel);
            
            // 保存连接和stub
            replicationInfo.setSlaveChannel(channel);
            replicationInfo.setSlaveAsyncStub(asyncStub);
            replicationInfo.setConnected(true);
            
            log.info("Successfully created gRPC connection to slave node {} at {}:{}", 
                    replicationInfo.getNodeId(), replicationInfo.getHost(), replicationInfo.getReplicationPort());
            
            // 创建持久的业务消息流
            createBusinessMessageStream(replicationInfo);
            
            // 延迟开始发送快照数据，确保连接稳定
            scheduler.schedule(() -> {
                try {
                    startSnapshotSync(replicationInfo);
                } catch (Exception e) {
                    log.error("Failed to start snapshot sync for node {}", replicationInfo.getNodeId(), e);
                    replicationInfo.setState(ReplicationState.ERROR);
                }
            }, 1, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            log.error("Failed to create connection to node {}: {}", replicationInfo.getNodeId(), e.getMessage());
            replicationInfo.setConnected(false);
            replicationInfo.setErrorMessage(e.getMessage());
            
            // 重试连接
            scheduler.schedule(() -> {
                log.info("Retrying connection to node {}", replicationInfo.getNodeId());
                createNodeConnection(replicationInfo);
            }, 5, TimeUnit.SECONDS);
        }
    }
    
    /**
     * 创建持久的业务消息流
     */
    private void createBusinessMessageStream(ReplicationInfo replicationInfo) {
        try {
            // 创建响应观察者
            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override
                public void onNext(ConfirmationMessage response) {
                    if (response.getSuccess()) {
                        log.debug("Business message confirmed by node {}", replicationInfo.getNodeId());
                    } else {
                        log.error("Business message failed for node {}: {}", replicationInfo.getNodeId(), response.getErrorMessage());
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    log.error("Business message stream error for node {}: {}", replicationInfo.getNodeId(), t.getMessage());
                    
                    // 如果是连接错误，尝试重新创建流
                    if (t.getMessage().contains("CANCELLED") || t.getMessage().contains("UNAVAILABLE")) {
                        log.info("Business stream error detected, will recreate stream for node {}", replicationInfo.getNodeId());
                        
                        // 延迟重新创建业务消息流
                        scheduler.schedule(() -> {
                            try {
                                createBusinessMessageStream(replicationInfo);
                            } catch (Exception e) {
                                log.error("Failed to recreate business stream for node {}", replicationInfo.getNodeId(), e);
                            }
                        }, 3, TimeUnit.SECONDS);
                    } else {
                        replicationInfo.setState(ReplicationState.ERROR);
                    }
                }
                
                @Override
                public void onCompleted() {
                    log.info("Business message stream completed for node {}", replicationInfo.getNodeId());
                    businessStreamObservers.remove(replicationInfo.getNodeId());
                }
            };
            
            // 创建请求观察者
            StreamObserver<BatchBusinessMessage> requestObserver = 
                    replicationInfo.getSlaveAsyncStub().sendBusiness(responseObserver);
            
            // 保存业务消息流观察者
            businessStreamObservers.put(replicationInfo.getNodeId(), requestObserver);
            
            log.info("Created persistent business message stream for node {}", replicationInfo.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to create business message stream for node {}", replicationInfo.getNodeId(), e);
        }
    }
    
    /**
     * 断开节点连接
     */
    private void disconnectNode(ReplicationInfo replicationInfo) {
        // 清理业务消息流
        StreamObserver<BatchBusinessMessage> businessObserver = businessStreamObservers.remove(replicationInfo.getNodeId());
        if (businessObserver != null) {
            try {
                businessObserver.onCompleted();
            } catch (Exception e) {
                log.warn("Failed to complete business stream for node {}", replicationInfo.getNodeId(), e);
            }
        }
        
        // 关闭gRPC连接
        if (replicationInfo.getSlaveChannel() != null && !replicationInfo.getSlaveChannel().isShutdown()) {
            replicationInfo.getSlaveChannel().shutdown();
        }
        
        replicationInfo.setConnected(false);
        replicationInfo.setState(ReplicationState.ERROR);
        log.info("Disconnected node {}", replicationInfo.getNodeId());
    }
    
    /**
     * 开始快照同步
     */
    private void startSnapshotSync(ReplicationInfo replicationInfo) {
        try {
            log.info("Starting snapshot sync for node {}", replicationInfo.getNodeId());
            
            // 更新状态为快照同步
            replicationInfo.setState(ReplicationState.SNAPSHOT_SYNC);
            
            // 创建快照数据流
            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override
                public void onNext(ConfirmationMessage response) {
                    if (response.getSuccess()) {
                        log.debug("Snapshot chunk confirmed by node {}", replicationInfo.getNodeId());
                    } else {
                        log.error("Snapshot chunk failed for node {}: {}", replicationInfo.getNodeId(), response.getErrorMessage());
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    log.error("Snapshot sync error for node {}: {}", replicationInfo.getNodeId(), t.getMessage());
                    
                    // 如果是连接错误，尝试重试
                    if (t.getMessage().contains("CANCELLED") || t.getMessage().contains("UNAVAILABLE")) {
                        log.info("Connection error detected, will retry snapshot sync for node {}", replicationInfo.getNodeId());
                        replicationInfo.setState(ReplicationState.REGISTERED);
                        
                        // 延迟重试快照同步
                        scheduler.schedule(() -> {
                            try {
                                startSnapshotSync(replicationInfo);
                            } catch (Exception e) {
                                log.error("Failed to retry snapshot sync for node {}", replicationInfo.getNodeId(), e);
                                replicationInfo.setState(ReplicationState.ERROR);
                            }
                        }, 3, TimeUnit.SECONDS);
                    } else {
                        replicationInfo.setState(ReplicationState.ERROR);
                    }
                }
                
                @Override
                public void onCompleted() {
                    log.info("Snapshot sync completed for node {}", replicationInfo.getNodeId());
                    replicationInfo.setState(ReplicationState.BUSINESS_BUFFERING);
                }
            };
            
            // 开始发送快照数据流
            StreamObserver<SnapshotDataMessage> requestObserver = 
                    replicationInfo.getSlaveAsyncStub().sendSnapshot(responseObserver);
            
            // TODO: 实际发送快照数据
            // 这里需要从实际的快照存储中读取数据并发送
            sendSnapshotData(requestObserver, replicationInfo);
            
        } catch (Exception e) {
            log.error("Failed to start snapshot sync for node {}", replicationInfo.getNodeId(), e);
            replicationInfo.setState(ReplicationState.ERROR);
        }
    }
    
    /**
     * 发送快照数据
     */
    private void sendSnapshotData(StreamObserver<SnapshotDataMessage> requestObserver, ReplicationInfo replicationInfo) {
        try {
            // TODO: 从实际的快照存储中读取数据
            // 这里只是示例，实际需要从文件系统或数据库读取快照数据
            
            SnapshotDataMessage snapshotMessage = SnapshotDataMessage.newBuilder()
                    .setSnapshotTimestamp(System.currentTimeMillis())
                    .setPartition(1)
                    .setDataType("SNAPSHOT")
                    .setSnapshotData(com.google.protobuf.ByteString.copyFromUtf8("Sample snapshot data"))
                    .setTotalPartitions(1)
                    .setIsLastPartition(true)
                    .build();
            
            requestObserver.onNext(snapshotMessage);
            requestObserver.onCompleted();
            
            log.info("Sent snapshot data to node {}", replicationInfo.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to send snapshot data to node {}", replicationInfo.getNodeId(), e);
            requestObserver.onError(e);
        }
    }
    
    /**
     * 更新节点心跳
     */
    public void updateHeartbeat(int nodeId) {
        ReplicationInfo replicationInfo = slaveNodes.get(nodeId);
        if (replicationInfo != null) {
            replicationInfo.updateHeartbeat();
        }
    }
    
    /**
     * 更新节点状态
     */
    public void updateNodeState(int nodeId, ReplicationState state) {
        ReplicationInfo replicationInfo = slaveNodes.get(nodeId);
        if (replicationInfo != null) {
            replicationInfo.setState(state);
            log.info("Updated node {} state to {}", nodeId, state);
        }
    }
    
    /**
     * 处理缓冲ID报告
     */
    public void handleBufferFirstIdReport(BufferFirstIdReportMessage reportMessage) {
        int nodeId = reportMessage.getNodeId();
        ReplicationInfo replicationInfo = slaveNodes.get(nodeId);
        
        if (replicationInfo != null) {
            replicationInfo.setFirstBufferedBusinessId(reportMessage.getFirstBufferedId());
            replicationInfo.setBufferSize(reportMessage.getBufferSize());
            replicationInfo.setState(ReplicationState.JOURNAL_SYNC);
            
            System.out.println("Slave node " + nodeId + " reported first buffered ID: " + 
                    reportMessage.getFirstBufferedId() + ", buffer size: " + reportMessage.getBufferSize());
            
            // 触发Journal同步
            triggerJournalSync(replicationInfo);
        }
    }
    
    /**
     * 触发Journal同步
     */
    private void triggerJournalSync(ReplicationInfo replicationInfo) {
        try {
            log.info("Triggering journal sync for node {} from sequence {} to {}", 
                    replicationInfo.getNodeId(), 1, replicationInfo.getFirstBufferedBusinessId());
            
            // 更新状态为Journal同步
            replicationInfo.setState(ReplicationState.JOURNAL_SYNC);
            
            // 创建Journal响应观察者
            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override
                public void onNext(ConfirmationMessage response) {
                    if (response.getSuccess()) {
                        log.debug("Journal chunk confirmed by node {}", replicationInfo.getNodeId());
                    } else {
                        log.error("Journal chunk failed for node {}: {}", replicationInfo.getNodeId(), response.getErrorMessage());
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    log.error("Journal sync error for node {}", replicationInfo.getNodeId(), t);
                    replicationInfo.setState(ReplicationState.ERROR);
                }
                
                @Override
                public void onCompleted() {
                    log.info("Journal sync completed for node {}", replicationInfo.getNodeId());
                    replicationInfo.setState(ReplicationState.READY);
                }
            };
            
            // 开始发送Journal数据流
            StreamObserver<JournalReplayMessage> requestObserver = 
                    replicationInfo.getSlaveAsyncStub().sendJournal(responseObserver);
            
            // 发送Journal数据
            sendJournalData(requestObserver, replicationInfo);
            
        } catch (Exception e) {
            log.error("Failed to trigger journal sync for node {}", replicationInfo.getNodeId(), e);
            replicationInfo.setState(ReplicationState.ERROR);
        }
    }
    
    /**
     * 发送Journal数据
     */
    private void sendJournalData(StreamObserver<JournalReplayMessage> requestObserver, ReplicationInfo replicationInfo) {
        try {
            // TODO: 从实际的Journal存储中读取数据
            // 这里只是示例，实际需要从Journal文件读取数据
            
            JournalReplayMessage journalMessage = JournalReplayMessage.newBuilder()
                    .setSequence(replicationInfo.getFirstBufferedBusinessId())
                    .setStartTimestamp(System.currentTimeMillis())
                    .setEndTimestamp(System.currentTimeMillis())
                    .setJournalData(com.google.protobuf.ByteString.copyFromUtf8("Sample journal data"))
                    .setIsLastChunk(true)
                    .build();
            
            requestObserver.onNext(journalMessage);
            requestObserver.onCompleted();
            
            log.info("Sent journal data to node {}", replicationInfo.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to send journal data to node {}", replicationInfo.getNodeId(), e);
            requestObserver.onError(e);
        }
    }
    
    /**
     * 发送业务消息到所有就绪的节点
     */
    public void sendBusinessMessage(BatchBusinessMessage businessMessage) {
        slaveNodes.values().stream()
                .filter(ReplicationInfo::isReady)
                .forEach(node -> sendBusinessMessageToNode(node, businessMessage));
    }
    
    /**
     * 发送业务消息到指定节点
     */
    private void sendBusinessMessageToNode(ReplicationInfo node, BatchBusinessMessage businessMessage) {
        try {
            // 获取持久的业务消息流观察者
            StreamObserver<BatchBusinessMessage> requestObserver = businessStreamObservers.get(node.getNodeId());
            
            if (requestObserver == null) {
                log.warn("No business stream observer available for node {}", node.getNodeId());
                return;
            }
            
            // 使用持久的流发送消息
            requestObserver.onNext(businessMessage);
            
            // 更新最后业务序列号
            node.setLastBusinessSequence(businessMessage.getEndSequence());
            
            log.debug("Sent business message to node {} via persistent stream", node.getNodeId());
            
        } catch (Exception e) {
            log.error("Failed to send business message to node {}", node.getNodeId(), e);
            node.setErrorMessage(e.getMessage());
        }
    }
    
    /**
     * 发送快照数据到节点
     */
    public void sendSnapshotToNode(int nodeId, SnapshotDataMessage snapshotMessage) {
        ReplicationInfo replicationInfo = slaveNodes.get(nodeId);
        if (replicationInfo != null) {
            try {
                // TODO: 实现实际的gRPC调用
                log.debug("Sending snapshot to node {}", nodeId);
                
                replicationInfo.setLastSnapshotSequence(snapshotMessage.getSnapshotTimestamp());
                
            } catch (Exception e) {
                log.error("Failed to send snapshot to node {}", nodeId, e);
                replicationInfo.setErrorMessage(e.getMessage());
            }
        }
    }
    
    /**
     * 检查心跳
     */
    private void checkHeartbeats() {
        LocalDateTime now = LocalDateTime.now();
        slaveNodes.values().forEach(replicationInfo -> {
            if (replicationInfo.getLastHeartbeat().isBefore(now.minusMinutes(2))) {
                log.warn("Node {} heartbeat timeout", replicationInfo.getNodeId());
                replicationInfo.setConnected(false);
                replicationInfo.setState(ReplicationState.ERROR);
            }
        });
    }
    
    /**
     * 同步状态
     */
    private void syncStates() {
        // TODO: 实现状态同步逻辑
        log.debug("Syncing states with {} nodes", slaveNodes.size());
    }
    
    /**
     * 获取节点信息
     */
    public ReplicationInfo getNodeInfo(int nodeId) {
        return slaveNodes.get(nodeId);
    }
    
    /**
     * 获取所有节点
     */
    public ConcurrentMap<Integer, ReplicationInfo> getAllNodes() {
        return new ConcurrentHashMap<>(slaveNodes);
    }
    
    /**
     * 获取就绪的节点数量
     */
    public int getReadyNodeCount() {
        return (int) slaveNodes.values().stream().filter(ReplicationInfo::isReady).count();
    }
    
    /**
     * 获取总节点数量
     */
    public int getTotalNodeCount() {
        return slaveNodes.size();
    }
    
    /**
     * 检查是否运行中
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * 设置MasterServer引用
     */
    public void setMasterServer(MasterServer masterServer) {
        this.masterServer = masterServer;
    }
}
