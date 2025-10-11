package com.cmex.bolt.replication;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.replication.ReplicationProto.*;
import com.lmax.disruptor.RingBuffer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 从节点复制管理器
 */
@Slf4j
public class SlaveReplicationManager {

    private final BoltConfig config;
    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    private volatile int assignedNodeId;
    private volatile ReplicationState currentState = ReplicationState.INITIAL;
    private volatile boolean isConnected = false;

    // MasterReplicationService stub
    private MasterReplicationServiceGrpc.MasterReplicationServiceBlockingStub masterStub;
    private MasterReplicationServiceGrpc.MasterReplicationServiceStub masterAsyncStub;
    private ManagedChannel masterChannel;

    // 中继消息缓冲
    private final BlockingQueue<BatchRelayMessage> relayMessageBuffer = new LinkedBlockingQueue<>();
    private final AtomicLong firstBufferedRelayId = new AtomicLong(-1);

    // 心跳和重连
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long lastHeartbeatTime = 0;

    public SlaveReplicationManager(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.config = config;
        this.assignedNodeId = config.nodeId();
        this.sequencerRingBuffer = sequencerRingBuffer;
    }

    /**
     * 启动从节点复制管理器
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            try {
                log.info("Starting slave replication manager for node {}", assignedNodeId);

                // 创建到主节点的连接
                createMasterConnection();

                // 注册到主节点
                registerToMaster();

                // 启动心跳任务
                startHeartbeat();

                isConnected = true;
                log.info("Slave replication manager started successfully");

            } catch (Exception e) {
                log.error("Failed to start slave replication manager", e);
                running.set(false);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 创建到主节点的连接
     */
    private void createMasterConnection() {
        masterChannel = ManagedChannelBuilder
                .forTarget(config.masterHost() + ":" + config.masterReplicationPort())
                .usePlaintext()
                .build();

        masterStub = MasterReplicationServiceGrpc.newBlockingStub(masterChannel);
        masterAsyncStub = MasterReplicationServiceGrpc.newStub(masterChannel);

        log.info("Created connection to master at {}:{}", config.masterHost(), config.masterReplicationPort());
    }

    /**
     * 停止从节点复制管理器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                isConnected = false;

                // 停止调度器
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    scheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                // 关闭主节点连接
                if (masterChannel != null && !masterChannel.isShutdown()) {
                    masterChannel.shutdown();
                    try {
                        masterChannel.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                log.info("Slave replication manager stopped");

            } catch (Exception e) {
                log.error("Failed to stop slave replication manager", e);
            }
        }
    }

    /**
     * 注册到主节点
     */
    private void registerToMaster() {
        try {
            // 获取本机IP地址
            String localHost = InetAddress.getLocalHost().getHostAddress();

            RegisterMessage registerMessage = RegisterMessage.newBuilder()
                    .setNodeId(assignedNodeId)
                    .setHost(localHost)
                    .setPort(config.port())
                    .setReplicationPort(config.slaveReplicationPort())
                    .putMetadata("role", "slave")
                    .putMetadata("version", "1.0")
                    .build();

            log.info("Registering to master with message: {}", registerMessage);

            RegisterResponse response = masterStub.registerSlave(registerMessage);

            if (response.getSuccess()) {
                // 更新分配的节点ID（主节点可能重新分配）
                this.assignedNodeId = response.getAssignedNodeId();
                currentState = ReplicationState.REGISTERED;
                log.info("Successfully registered to master with assigned node ID: {}", assignedNodeId);
            } else {
                throw new RuntimeException("Registration failed: " + response.getMessage());
            }

        } catch (Exception e) {
            log.error("Failed to register to master", e);
            currentState = ReplicationState.ERROR;
            throw new RuntimeException(e);
        }
    }

    /**
     * 启动心跳任务
     */
    private void startHeartbeat() {
        // 每30秒发送一次心跳
        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 30, 30, TimeUnit.SECONDS);

        // 每10秒检查连接状态
        scheduler.scheduleAtFixedRate(this::checkConnection, 10, 10, TimeUnit.SECONDS);

        log.info("Heartbeat tasks started");
    }

    /**
     * 发送心跳到主节点
     */
    private void sendHeartbeat() {
        if (!isConnected || currentState == ReplicationState.ERROR) {
            return;
        }

        try {
            StreamObserver<HeartbeatMessage> requestObserver = masterAsyncStub.heartbeat(
                new StreamObserver<HeartbeatResponse>() {
                    @Override
                    public void onNext(HeartbeatResponse response) {
                        lastHeartbeatTime = System.currentTimeMillis();
                        log.info("Received heartbeat response from master");
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error("Heartbeat stream error: {}", t.getMessage());
                        currentState = ReplicationState.ERROR;
                    }

                    @Override
                    public void onCompleted() {
                        log.info("Heartbeat stream completed");
                    }
                }
            );

            HeartbeatMessage heartbeat = HeartbeatMessage.newBuilder()
                    .setNodeId(assignedNodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .setSequence(0) // TODO: 使用实际的序列号
                    .build();

            requestObserver.onNext(heartbeat);
            requestObserver.onCompleted();

        } catch (Exception e) {
            log.error("Failed to send heartbeat: {}", e.getMessage());
            currentState = ReplicationState.ERROR;
        }
    }

    /**
     * 检查连接状态
     */
    private void checkConnection() {
        if (!isConnected) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        if (lastHeartbeatTime > 0 && (currentTime - lastHeartbeatTime) > 60000) { // 60秒超时
            log.warn("Heartbeat timeout detected, attempting reconnection");
            attemptReconnection();
        }
    }

    /**
     * 尝试重连
     */
    private void attemptReconnection() {
        try {
            log.info("Attempting to reconnect to master...");

            // 关闭现有连接
            if (masterChannel != null && !masterChannel.isShutdown()) {
                masterChannel.shutdown();
            }

            // 重新创建连接
            createMasterConnection();

            // 重新注册
            registerToMaster();

            log.info("Successfully reconnected to master");

        } catch (Exception e) {
            log.error("Failed to reconnect to master: {}", e.getMessage());
            currentState = ReplicationState.ERROR;
        }
    }

    /**
     * 处理中继消息
     */
    public boolean processRelayMessage(BatchRelayMessage relayMessage) {
        try {
            if (currentState == ReplicationState.RELAY_BUFFERING) {
                // 缓冲中继消息
                relayMessageBuffer.offer(relayMessage);

                // 记录第一个缓冲的中继消息ID
                if (firstBufferedRelayId.get() == -1) {
                    firstBufferedRelayId.set(relayMessage.getSequence());

                    // 报告缓冲ID给主节点
                    reportBufferFirstId();
                }

                log.debug("Buffered relay message batch {}", relayMessage.getSequence());
                return true;

            } else if (currentState == ReplicationState.READY) {
                // 直接处理中继消息
                return processRelayMessageDirectly(relayMessage);

            } else {
                log.warn("Received relay message in state {}, ignoring", currentState);
                return false;
            }

        } catch (Exception e) {
            log.error("Failed to process relay message", e);
            return false;
        }
    }

    /**
     * 直接处理中继消息
     */
    private boolean processRelayMessageDirectly(BatchRelayMessage relayMessage) {
        try {
            log.debug("Processing relay message directly: {}", relayMessage.getSequence());
            
            // 处理每个消息数据
            for (RelayMessageData messageData : relayMessage.getMessagesList()) {
                // 创建NexusWrapper并设置元数据
                sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                    // 设置元数据
                    wrapper.setId(messageData.getId());
                    wrapper.setPartition(messageData.getPartition());
                    wrapper.setEventType(NexusWrapper.EventType.fromValue(messageData.getEventType()));
                    
                    // 写入数据
                    wrapper.getBuffer().writeBytes(messageData.getData().toByteArray());
                    
                    log.debug("Processed relay message: id={}, partition={}, eventType={}, dataSize={}", 
                            messageData.getId(), messageData.getPartition(), 
                            messageData.getEventType(), messageData.getData().size());
                });
            }
            
            return true;
        } catch (Exception e) {
            log.error("Failed to process relay message directly", e);
            return false;
        }
    }

    /**
     * 处理快照数据
     */
    public void processSnapshotData(SnapshotReplayMessage snapshotMessage) {
        try {
//            log.info("Processing snapshot data: {}", snapshotMessage.getSnapshotTimestamp());
//            // TODO: 实现实际的快照处理逻辑
//
//            if (snapshotMessage.getIsLastPartition()) {
//                currentState = ReplicationState.BUSINESS_BUFFERING;
//                log.info("Snapshot sync completed, transitioning to BUSINESS_BUFFERING");
//            }

        } catch (Exception e) {
            log.error("Failed to process snapshot data", e);
            currentState = ReplicationState.ERROR;
        }
    }

    /**
     * 处理Journal数据
     */
    public void processJournalData(JournalReplayMessage journalMessage) {
        try {
//            log.info("Processing journal data: {}", journalMessage.getSequence());
//            // TODO: 实现实际的Journal处理逻辑
//
//            if (journalMessage.getIsLastChunk()) {
//                log.info("Journal sync completed, publishing buffered business messages");
//                publishBufferedBusinessMessages();
                currentState = ReplicationState.READY;
//                log.info("Slave is now READY");
//            }

        } catch (Exception e) {
            log.error("Failed to process journal data", e);
            currentState = ReplicationState.ERROR;
        }
    }

    /**
     * 发布缓冲的中继消息
     */
    public void publishBufferedRelayMessages() {
        log.info("Publishing {} buffered relay messages", relayMessageBuffer.size());

        while (!relayMessageBuffer.isEmpty()) {
            BatchRelayMessage message = relayMessageBuffer.poll();
            if (message != null) {
                processRelayMessageDirectly(message);
            }
        }

        // 确认中继消息发布
        confirmRelayPublish();

        // 更新状态为就绪
        currentState = ReplicationState.READY;
    }

    /**
     * 报告缓冲第一条数据ID
     */
    private void reportBufferFirstId() {
        try {
            BufferFirstIdReportMessage reportMessage = BufferFirstIdReportMessage.newBuilder()
                    .setNodeId(assignedNodeId)
                    .setFirstBufferedId(firstBufferedRelayId.get())
                    .setTimestamp(System.currentTimeMillis())
                    .setBufferSize(relayMessageBuffer.size())
                    .build();

            ConfirmationMessage response = masterStub.reportBufferFirstId(reportMessage);

            if (response.getSuccess()) {
                log.info("Successfully reported buffer first ID: {}", firstBufferedRelayId.get());
            } else {
                log.error("Failed to report buffer first ID: {}", response.getErrorMessage());
            }

        } catch (Exception e) {
            log.error("Failed to report buffer first ID", e);
        }
    }

    /**
     * 确认中继消息发布
     */
    private void confirmRelayPublish() {
        try {
            RelayPublishConfirmMessage confirmMessage = RelayPublishConfirmMessage.newBuilder()
                    .setNodeId(assignedNodeId)
                    .setPublishedSequence(firstBufferedRelayId.get())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(true)
                    .build();

            ConfirmationMessage response = masterStub.confirmRelayPublish(confirmMessage);

            if (response.getSuccess()) {
                log.info("Successfully confirmed relay publish");
            } else {
                log.error("Failed to confirm relay publish: {}", response.getErrorMessage());
            }

        } catch (Exception e) {
            log.error("Failed to confirm relay publish", e);
        }
    }

    /**
     * 更新状态
     */
    public void updateState(ReplicationState newState) {
        this.currentState = newState;
        log.info("State updated to: {}", newState);
    }

    // Getters
    public int getAssignedNodeId() {
        return assignedNodeId;
    }

    public ReplicationState getCurrentState() {
        return currentState;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public boolean isRunning() {
        return running.get();
    }

    public MasterReplicationServiceGrpc.MasterReplicationServiceBlockingStub getMasterStub() {
        return masterStub;
    }

    public MasterReplicationServiceGrpc.MasterReplicationServiceStub getMasterAsyncStub() {
        return masterAsyncStub;
    }
}
