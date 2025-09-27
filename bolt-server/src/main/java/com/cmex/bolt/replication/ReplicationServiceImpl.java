package com.cmex.bolt.replication;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.handler.ReplicationHandler;
import com.cmex.bolt.replication.ReplicationServiceProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * gRPC复制服务实现
 * 主节点提供复制服务给从节点
 */
@Slf4j
public class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {
    
    private final ReplicationHandler replicationHandler;
    private final AtomicLong nextBatchId = new AtomicLong(1);
    
    // 存储从节点的连接信息
    private final ConcurrentHashMap<String, SlaveConnection> slaveConnections = new ConcurrentHashMap<>();
    
    public ReplicationServiceImpl(ReplicationHandler replicationHandler) {
        this.replicationHandler = replicationHandler;
    }
    
    /**
     * 从节点注册
     */
    @Override
    public void registerSlave(RegisterSlaveRequest request, StreamObserver<RegisterSlaveResponse> responseObserver) {
        String slaveNodeId = request.getSlaveNodeId();
        String slaveHost = request.getSlaveHost();
        int slavePort = request.getSlavePort();
        long lastSequence = request.getLastSequence();
        
        log.info("Received slave registration request: {} at {}:{} with last sequence {}", 
                slaveNodeId, slaveHost, slavePort, lastSequence);
        
        try {
            // 注册从节点
            log.debug("Registering slave {} with ReplicationHandler", slaveNodeId);
            replicationHandler.registerSlave(slaveNodeId, slaveHost, slavePort);
            
            // 创建连接信息
            SlaveConnection connection = new SlaveConnection(slaveNodeId, slaveHost, slavePort, lastSequence);
            slaveConnections.put(slaveNodeId, connection);
            log.debug("Stored connection info for slave {}", slaveNodeId);
            
            // 返回成功响应
            RegisterSlaveResponse response = RegisterSlaveResponse.newBuilder()
                    .setSuccess(true)
                    .setStartSequence(lastSequence + 1) // 从下一个序列开始
                    .build();
            
            log.info("Successfully registered slave {} at {}:{} with start sequence {}", 
                    slaveNodeId, slaveHost, slavePort, lastSequence + 1);
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to register slave {} at {}:{}: {}", slaveNodeId, slaveHost, slavePort, e.getMessage(), e);
            
            RegisterSlaveResponse response = RegisterSlaveResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Registration failed: " + e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 推送事件批次到从节点
     */
    @Override
    public void pushEventBatch(EventBatchRequest request, StreamObserver<EventBatchResponse> responseObserver) {
        try {
            long batchId = request.getBatchId();
            long sequenceStart = request.getSequenceStart();
            long sequenceEnd = request.getSequenceEnd();
            
            log.debug("Received batch {} from master, sequences {}-{}", batchId, sequenceStart, sequenceEnd);
            
            // 转换NexusEvent到NexusWrapper
            List<NexusWrapper> events = convertToNexusWrappers(request.getEventsList());
            
            // 处理事件批次（这里应该写入从节点的本地RingBuffer）
            // 暂时先记录日志
            log.debug("Processing {} events in batch {}", events.size(), batchId);
            
            // 模拟处理完成
            Thread.sleep(10); // 模拟处理时间
            
            // 发送确认
            sendBatchAcknowledgment(batchId, sequenceEnd);
            
            EventBatchResponse response = EventBatchResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to process event batch", e);
            
            EventBatchResponse response = EventBatchResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Batch processing failed: " + e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 处理从节点的批次确认
     */
    @Override
    public void acknowledgeBatch(BatchAckRequest request, StreamObserver<BatchAckResponse> responseObserver) {
        try {
            long batchId = request.getBatchId();
            String slaveNodeId = request.getSlaveNodeId();
            long processedSequence = request.getProcessedSequence();
            
            log.debug("Received ack for batch {} from slave {} at sequence {}", 
                    batchId, slaveNodeId, processedSequence);
            
            // 处理确认
            replicationHandler.handleBatchAcknowledgment(batchId, slaveNodeId);
            
            BatchAckResponse response = BatchAckResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to process batch acknowledgment", e);
            
            BatchAckResponse response = BatchAckResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage("Acknowledgment processing failed: " + e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 处理心跳
     */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            String slaveNodeId = request.getSlaveNodeId();
            
            log.debug("Received heartbeat from slave {}", slaveNodeId);
            
            // 更新心跳
            replicationHandler.handleSlaveHeartbeat(slaveNodeId);
            
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setSuccess(true)
                    .setMasterTimestamp(System.currentTimeMillis())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to process heartbeat", e);
            
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setSuccess(false)
                    .setMasterTimestamp(System.currentTimeMillis())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 转换NexusEvent到NexusWrapper
     */
    private List<NexusWrapper> convertToNexusWrappers(List<ReplicationServiceProto.NexusEvent> events) {
        List<NexusWrapper> wrappers = new ArrayList<>();
        
        for (ReplicationServiceProto.NexusEvent event : events) {
            // 这里需要根据实际的NexusWrapper构造函数来创建
            // 暂时创建一个空的实现
            log.debug("Converting event {} to NexusWrapper", event.getId());
        }
        
        return wrappers;
    }
    
    /**
     * 发送批次确认
     */
    private void sendBatchAcknowledgment(long batchId, long processedSequence) {
        // 这里应该发送确认到主节点
        // 暂时先记录日志
        log.debug("Sending batch acknowledgment for batch {} at sequence {}", batchId, processedSequence);
    }
    
    /**
     * 双向流复制服务
     */
    @Override
    public StreamObserver<ReplicationMessage> replicationStream(StreamObserver<ReplicationMessage> responseObserver) {
        return new StreamObserver<ReplicationMessage>() {
            private String slaveNodeId = null;
            
            @Override
            public void onNext(ReplicationMessage message) {
                try {
                    switch (message.getMessageTypeCase()) {
                        case EVENT_BATCH:
                            handleEventBatch(message.getEventBatch(), responseObserver);
                            break;
                        case BATCH_ACK:
                            handleBatchAck(message.getBatchAck());
                            break;
                        case HEARTBEAT:
                            handleHeartbeat(message.getHeartbeat());
                            break;
                        case SLAVE_REGISTER:
                            handleSlaveRegister(message.getSlaveRegister(), responseObserver);
                            break;
                        default:
                            log.warn("Unknown message type: {}", message.getMessageTypeCase());
                    }
                } catch (Exception e) {
                    log.error("Error processing replication message", e);
                    sendErrorResponse(responseObserver, "Error processing message: " + e.getMessage());
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Replication stream error for slave {}", slaveNodeId, t);
                if (slaveNodeId != null) {
                    replicationHandler.unregisterSlave(slaveNodeId);
                }
            }
            
            @Override
            public void onCompleted() {
                log.info("Replication stream completed for slave {}", slaveNodeId);
                if (slaveNodeId != null) {
                    replicationHandler.unregisterSlave(slaveNodeId);
                }
            }
            
            private void handleEventBatch(EventBatchRequest request, StreamObserver<ReplicationMessage> responseObserver) {
                // 处理从节点发送的事件批次（通常不会发生，因为主节点是发送方）
                log.debug("Received event batch from slave: {}", request.getBatchId());
            }
            
            private void handleBatchAck(BatchAckRequest request) {
                replicationHandler.handleBatchAcknowledgment(request.getBatchId(), request.getSlaveNodeId());
            }
            
            private void handleHeartbeat(HeartbeatRequest request) {
                if (slaveNodeId == null) {
                    slaveNodeId = request.getSlaveNodeId();
                }
                replicationHandler.handleSlaveHeartbeat(request.getSlaveNodeId());
            }
            
            private void handleSlaveRegister(RegisterSlaveRequest request, StreamObserver<ReplicationMessage> responseObserver) {
                slaveNodeId = request.getSlaveNodeId();
                replicationHandler.registerSlave(request.getSlaveNodeId(), request.getSlaveHost(), request.getSlavePort());
                
                ReplicationMessage response = ReplicationMessage.newBuilder()
                        .setResponse(ReplicationResponse.newBuilder()
                                .setSuccess(true)
                                .setTimestamp(System.currentTimeMillis())
                                .build())
                        .build();
                responseObserver.onNext(response);
            }
            
            private void sendErrorResponse(StreamObserver<ReplicationMessage> responseObserver, String errorMessage) {
                ReplicationMessage response = ReplicationMessage.newBuilder()
                        .setResponse(ReplicationResponse.newBuilder()
                                .setSuccess(false)
                                .setErrorMessage(errorMessage)
                                .setTimestamp(System.currentTimeMillis())
                                .build())
                        .build();
                responseObserver.onNext(response);
            }
        };
    }
    
    /**
     * 从节点连接信息
     */
    private static class SlaveConnection {
        private final String nodeId;
        private final String host;
        private final int port;
        private final long lastSequence;
        private volatile long lastHeartbeat;
        
        public SlaveConnection(String nodeId, String host, int port, long lastSequence) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.lastSequence = lastSequence;
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public boolean isHealthy(long timeoutMs) {
            return (System.currentTimeMillis() - lastHeartbeat) < timeoutMs;
        }
        
        // Getters
        public String getNodeId() { return nodeId; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public long getLastSequence() { return lastSequence; }
        public long getLastHeartbeat() { return lastHeartbeat; }
    }
}
