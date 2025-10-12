package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 主节点服务器，整合了复制管理功能
 */
@Slf4j
public class MasterServer {
    
    private final BoltConfig config;
    private final ReplicationMasterServiceImpl masterService;
    private Server server;
    
    // 存储从节点的stub连接
    private final ConcurrentMap<Integer, ReplicationSlaveServiceGrpc.ReplicationSlaveServiceBlockingStub> slaveStubs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub> slaveAsyncStubs = new ConcurrentHashMap<>();
    
    // 存储所有slave节点的信息
    private final ConcurrentMap<Integer, ReplicationInfo> slaveNodes = new ConcurrentHashMap<>();
    
    // 定时任务执行器
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    // 运行状态
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    public MasterServer(BoltConfig boltConfig) {
        this.config = boltConfig;
        this.masterService = new ReplicationMasterServiceImpl(boltConfig, this);
    }
    
    /**
     * 启动主节点服务器
     */
    public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            log.info("Starting MasterServer");
            
            server = ServerBuilder.forPort(config.masterReplicationPort())
                    .addService(masterService)
                    .build()
                    .start();

            log.info("Replication Master server started, listening on port {}", config.masterReplicationPort());
            
            // 启动心跳检查任务
            // scheduler.scheduleWithFixedDelay(this::checkHeartbeats, 30, 30, TimeUnit.SECONDS);
            
            // 启动状态同步任务
            // scheduler.scheduleWithFixedDelay(this::syncStates, 10, 10, TimeUnit.SECONDS);
            
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
            
            log.info("MasterServer started successfully");
        }
    }
    
    /**
     * 停止主节点服务器
     */
    public void stop() throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping MasterServer");
            
            if (server != null) {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            }
            
            // 关闭定时任务执行器
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            // 关闭所有从节点连接
            slaveStubs.clear();
            slaveAsyncStubs.clear();
            
            log.info("MasterServer stopped");
        }
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
            ReplicationSlaveServiceGrpc.ReplicationSlaveServiceBlockingStub slaveStub =
                    ReplicationSlaveServiceGrpc.newBlockingStub(slaveChannel);
            ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub slaveAsyncStub =
                    ReplicationSlaveServiceGrpc.newStub(slaveChannel);
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
    public ReplicationSlaveServiceGrpc.ReplicationSlaveServiceBlockingStub getSlaveStub(int nodeId) {
        return slaveStubs.get(nodeId);
    }
    
    /**
     * 获取从节点异步stub
     */
    public ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub getSlaveAsyncStub(int nodeId) {
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
    
    /**
     * 注册节点
     */
    public boolean registerNode(RegisterMessage registerMessage) {
        try {
            int nodeId = registerMessage.getNodeId();
            String host = registerMessage.getHost();
            int replicationPort = registerMessage.getReplicationPort();

            ReplicationInfo replicationInfo = new ReplicationInfo(nodeId, host, replicationPort);
            replicationInfo.setState(ReplicationState.REGISTERED);
            replicationInfo.setConnected(true);

            slaveNodes.put(nodeId, replicationInfo);

            log.info("Node {} registered successfully: {}", nodeId, host);
            createNodeConnection(replicationInfo);
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
                    .keepAliveTimeout(30, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();

            // 创建异步stub
            ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub asyncStub =
                    ReplicationSlaveServiceGrpc.newStub(channel)
                            .withDeadlineAfter(60, TimeUnit.SECONDS);

            // 保存连接和stub
            replicationInfo.setSlaveChannel(channel);
            replicationInfo.setSlaveAsyncStub(asyncStub);
            replicationInfo.setConnected(true);

            log.info("Successfully created gRPC connection to slave node {} at {}:{}",
                    replicationInfo.getNodeId(), replicationInfo.getHost(), replicationInfo.getReplicationPort());

        } catch (Exception e) {
            log.error("Failed to create connection to node {}: {}", replicationInfo.getNodeId(), e.getMessage());
            replicationInfo.setConnected(false);
            replicationInfo.setErrorMessage(e.getMessage());
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
     * 处理最新复制ID报告
     */
    public void handleLatestReplicationReport(LatestReplicationMessage reportMessage) {
        int nodeId = reportMessage.getNodeId();
        ReplicationInfo replicationInfo = slaveNodes.get(nodeId);

        if (replicationInfo != null) {
            replicationInfo.setFirstReplicationId(reportMessage.getReplicationId());
            replicationInfo.setState(ReplicationState.REGISTERED);

            log.info("Slave node {} reported latest replication ID: {}",
                    nodeId, reportMessage.getReplicationId());

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
                    replicationInfo.getNodeId(), 1, replicationInfo.getFirstReplicationId());

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

            log.info("stub info {} , {}", replicationInfo.getSlaveAsyncStub(), replicationInfo.getSlaveChannel().isTerminated());
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
            // 使用JournalReader读取journal数据
            JournalReader journalReader = new JournalReader(config);
            byte[] journalData = journalReader.readJournalToReplicationId(replicationInfo.getFirstReplicationId());
            
            if (journalData.length == 0) {
                log.warn("No journal data found for replication ID {}", replicationInfo.getFirstReplicationId());
            } else {
                log.info("Read {} bytes of journal data for replication ID {}", 
                        journalData.length, replicationInfo.getFirstReplicationId());
            }

            JournalReplayMessage journalMessage = JournalReplayMessage.newBuilder()
                    .setJournalData(com.google.protobuf.ByteString.copyFrom(journalData))
                    .setIsLastChunk(true)
                    .build();

            requestObserver.onNext(journalMessage);
            requestObserver.onCompleted();

            log.info("Sent journal data to node {}: {} bytes", replicationInfo.getNodeId(), journalData.length);

        } catch (Exception e) {
            log.error("Failed to send journal data to node {}", replicationInfo.getNodeId(), e);
            requestObserver.onError(e);
        }
    }

    /**
     * 发送中继消息到所有就绪的节点
     */
    public void sendRelayMessage(BatchRelayMessage relayMessage) {
        slaveNodes.values().stream()
                .filter(ReplicationInfo::isConnected)
                .forEach(node -> sendRelayMessageToNode(node, relayMessage));
    }

    /**
     * 发送中继消息到指定节点
     */
    private void sendRelayMessageToNode(ReplicationInfo node, BatchRelayMessage relayMessage) {
        try {
            // 检查节点是否已连接
            if (!node.isConnected() || node.getSlaveAsyncStub() == null) {
                log.warn("Node {} is not connected or stub is null", node.getNodeId());
                return;
            }

            // 创建响应观察者
            StreamObserver<ConfirmationMessage> responseObserver = new StreamObserver<ConfirmationMessage>() {
                @Override
                public void onNext(ConfirmationMessage response) {
                    if (response.getSuccess()) {
                        log.debug("Relay message confirmed by node {} for sequence {}", 
                                node.getNodeId(), response.getSequence());
                        // 更新最后中继序列号
                        node.setLastRelaySequence(response.getSequence());
                    } else {
                        log.error("Relay message failed for node {}: {}", 
                                node.getNodeId(), response.getErrorMessage());
                        node.setErrorMessage(response.getErrorMessage());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Relay message stream error for node {}: {}", 
                            node.getNodeId(), t.getMessage());
                    node.setState(ReplicationState.ERROR);
                    node.setErrorMessage(t.getMessage());
                }

                @Override
                public void onCompleted() {
                    log.debug("Relay message stream completed for node {}", node.getNodeId());
                }
            };

            // 创建请求观察者并发送消息
            StreamObserver<BatchRelayMessage> requestObserver = 
                    node.getSlaveAsyncStub().sendRelay(responseObserver);

            // 发送中继消息
            requestObserver.onNext(relayMessage);
            requestObserver.onCompleted();

            log.debug("Sent relay message to node {} with sequence {}", 
                    node.getNodeId(), relayMessage.getSequence());

        } catch (Exception e) {
            log.error("Failed to send relay message to node {}", node.getNodeId(), e);
            node.setErrorMessage(e.getMessage());
            node.setState(ReplicationState.ERROR);
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
        return slaveNodes.size();
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
}
