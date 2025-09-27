package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationServiceGrpc;
import com.cmex.bolt.replication.ReplicationServiceProto;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 复制客户端 - 负责与主节点的通信
 */
@Slf4j
public class ReplicationClient {
    
    private final String host;
    private final int port;
    private final String slaveNodeId;
    private ManagedChannel channel;
    private ReplicationServiceGrpc.ReplicationServiceBlockingStub blockingStub;
    private ReplicationServiceGrpc.ReplicationServiceStub asyncStub;
    private StreamObserver<ReplicationServiceProto.ReplicationMessage> requestObserver;
    private volatile boolean connected = false;
    
    public ReplicationClient(String host, int port, String slaveNodeId) {
        this.host = host;
        this.port = port;
        this.slaveNodeId = slaveNodeId;
    }
    
    /**
     * 连接到主节点
     */
    public void connect() {
        log.info("Attempting to connect to master at {}:{} for slave {}", host, port, slaveNodeId);
        
        try {
            // 使用IP地址直接连接，避免localhost解析问题
            String target = host.equals("localhost") ? "127.0.0.1:" + port : host + ":" + port;
            channel = ManagedChannelBuilder.forTarget(target)
                    .usePlaintext()
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(5, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                    .defaultLoadBalancingPolicy("round_robin") // 使用round_robin替代pick_first
                    .build();
            
            blockingStub = ReplicationServiceGrpc.newBlockingStub(channel);
            asyncStub = ReplicationServiceGrpc.newStub(channel);
            
            // 建立双向流连接
            requestObserver = asyncStub.replicationStream(new StreamObserver<ReplicationServiceProto.ReplicationMessage>() {
                @Override
                public void onNext(ReplicationServiceProto.ReplicationMessage message) {
                    handleReplicationMessage(message);
                }
                
                @Override
                public void onError(Throwable t) {
                    log.error("Replication stream error", t);
                    connected = false;
                }
                
                @Override
                public void onCompleted() {
                    log.info("Replication stream completed");
                    connected = false;
                }
            });
            
            connected = true;
            log.info("Successfully connected to master at {}:{} for slave {}", host, port, slaveNodeId);
        } catch (Exception e) {
            log.error("Failed to connect to master at {}:{} for slave {}: {}", host, port, slaveNodeId, e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * 发送批次到主节点
     */
    public void sendBatch(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        if (channel == null || channel.isShutdown()) {
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            ReplicationServiceProto.EventBatchRequest.Builder requestBuilder = 
                    ReplicationServiceProto.EventBatchRequest.newBuilder()
                            .setBatchId(batchId)
                            .setSequenceStart(sequenceStart)
                            .setSequenceEnd(sequenceEnd)
                            .setTimestamp(System.currentTimeMillis());
            
            // 转换NexusWrapper到NexusEvent
            for (NexusWrapper wrapper : events) {
                ReplicationServiceProto.NexusEvent nexusEvent = ReplicationServiceProto.NexusEvent.newBuilder()
                        .setId(wrapper.getId())
                        .setPartition(wrapper.getPartition())
                        .setData(ByteString.copyFrom(wrapper.getBuffer().nioBuffer()))
                        .build();
                requestBuilder.addEvents(nexusEvent);
            }
            
            ReplicationServiceProto.EventBatchRequest request = requestBuilder.build();
            
            // 异步发送，避免阻塞
            asyncStub.pushEventBatch(request, new StreamObserver<ReplicationServiceProto.EventBatchResponse>() {
                @Override
                public void onNext(ReplicationServiceProto.EventBatchResponse response) {
                    if (response.getSuccess()) {
                        log.debug("Successfully sent batch {} to master", batchId);
                    } else {
                        log.error("Failed to send batch {} to master: {}", batchId, response.getErrorMessage());
                    }
                }
                
                @Override
                public void onError(Throwable t) {
                    log.error("Error sending batch {} to master", batchId, t);
                }
                
                @Override
                public void onCompleted() {
                    log.debug("Batch {} send completed", batchId);
                }
            });
            
        } catch (Exception e) {
            log.error("Failed to send batch {} to master", batchId, e);
            throw e;
        }
    }
    
    /**
     * 发送批次确认（旧方法，保留兼容性）
     */
    public void sendBatchAcknowledgment(long batchId, long processedSequence) {
        if (requestObserver == null) {
            log.warn("Request observer not available for batch acknowledgment");
            return;
        }
        
        ReplicationServiceProto.ReplicationMessage message = ReplicationServiceProto.ReplicationMessage.newBuilder()
                .setBatchAck(ReplicationServiceProto.BatchAckRequest.newBuilder()
                        .setBatchId(batchId)
                        .setSlaveNodeId(slaveNodeId)
                        .setProcessedSequence(processedSequence)
                        .setTimestamp(System.currentTimeMillis())
                        .build())
                .build();
        
        requestObserver.onNext(message);
    }
    
    /**
     * 发送心跳
     */
    public void sendHeartbeat() {
        if (requestObserver == null) {
            log.warn("Request observer not available for heartbeat");
            return;
        }
        
        try {
            ReplicationServiceProto.ReplicationMessage message = ReplicationServiceProto.ReplicationMessage.newBuilder()
                    .setHeartbeat(ReplicationServiceProto.HeartbeatRequest.newBuilder()
                            .setSlaveNodeId(slaveNodeId)
                            .setTimestamp(System.currentTimeMillis())
                            .build())
                    .build();
            
            requestObserver.onNext(message);
            log.debug("Heartbeat sent to master");
            
        } catch (Exception e) {
            log.error("Failed to send heartbeat to master", e);
            throw e;
        }
    }
    
    /**
     * 注册从节点
     */
    public void registerSlave(String slaveHost, int slavePort, long lastSequence) {
        if (requestObserver == null) {
            throw new IllegalStateException("Client not connected or stream not established");
        }
        
        try {
            ReplicationServiceProto.ReplicationMessage message = ReplicationServiceProto.ReplicationMessage.newBuilder()
                    .setSlaveRegister(ReplicationServiceProto.RegisterSlaveRequest.newBuilder()
                            .setSlaveNodeId(slaveNodeId)
                            .setSlaveHost(slaveHost)
                            .setSlavePort(slavePort)
                            .setLastSequence(lastSequence)
                            .build())
                    .build();
            
            requestObserver.onNext(message);
            log.info("Sent slave registration request for {} at {}:{}", slaveNodeId, slaveHost, slavePort);
            
        } catch (Exception e) {
            log.error("Failed to register slave {} with master", slaveNodeId, e);
            throw e;
        }
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        log.info("Attempting to disconnect from master at {}:{} for slave {}", host, port, slaveNodeId);
        
        connected = false;
        
        if (requestObserver != null) {
            try {
                requestObserver.onCompleted();
            } catch (Exception e) {
                log.warn("Error completing request observer", e);
            }
            requestObserver = null;
        }
        
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.info("Successfully disconnected from master at {}:{} for slave {}", host, port, slaveNodeId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while disconnecting from master at {}:{} for slave {}", host, port, slaveNodeId, e);
            }
        } else {
            log.debug("Channel already shutdown or null for slave {}", slaveNodeId);
        }
    }
    
    /**
     * 处理从主节点接收的复制消息
     */
    private void handleReplicationMessage(ReplicationServiceProto.ReplicationMessage message) {
        switch (message.getMessageTypeCase()) {
            case EVENT_BATCH:
                handleEventBatch(message.getEventBatch());
                break;
            case RESPONSE:
                handleResponse(message.getResponse());
                break;
            default:
                log.debug("Received message type: {}", message.getMessageTypeCase());
        }
    }
    
    /**
     * 处理事件批次
     */
    private void handleEventBatch(ReplicationServiceProto.EventBatchRequest request) {
        log.info("Received event batch {} from master", request.getBatchId());
        // 这里应该处理事件批次，写入本地RingBuffer
        // 然后发送确认
        sendBatchAcknowledgment(request.getBatchId(), request.getSequenceEnd());
    }
    
    /**
     * 处理响应消息
     */
    private void handleResponse(ReplicationServiceProto.ReplicationResponse response) {
        if (response.getSuccess()) {
            log.debug("Received success response from master");
        } else {
            log.warn("Received error response from master: {}", response.getErrorMessage());
        }
    }
    
    
    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return connected && channel != null && !channel.isShutdown();
    }
    
    /**
     * 获取从节点ID
     */
    public String getSlaveNodeId() {
        return slaveNodeId;
    }
}
