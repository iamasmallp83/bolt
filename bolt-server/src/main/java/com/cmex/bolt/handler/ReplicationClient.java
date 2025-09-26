package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationServiceGrpc;
import com.cmex.bolt.replication.ReplicationServiceProto;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

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
    
    public ReplicationClient(String host, int port, String slaveNodeId) {
        this.host = host;
        this.port = port;
        this.slaveNodeId = slaveNodeId;
    }
    
    /**
     * 连接到主节点
     */
    public void connect() {
        channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .build();
        
        blockingStub = ReplicationServiceGrpc.newBlockingStub(channel);
        asyncStub = ReplicationServiceGrpc.newStub(channel);
        
        log.info("Connected to master at {}:{}", host, port);
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
     * 发送批次确认
     */
    public void sendBatchAcknowledgment(long batchId, long processedSequence) {
        if (channel == null || channel.isShutdown()) {
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            ReplicationServiceProto.BatchAckRequest request = ReplicationServiceProto.BatchAckRequest.newBuilder()
                    .setBatchId(batchId)
                    .setSlaveNodeId(slaveNodeId)
                    .setProcessedSequence(processedSequence)
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            ReplicationServiceProto.BatchAckResponse response = blockingStub.acknowledgeBatch(request);
            
            if (response.getSuccess()) {
                log.debug("Successfully acknowledged batch {} to master", batchId);
            } else {
                log.error("Failed to acknowledge batch {} to master: {}", batchId, response.getErrorMessage());
            }
            
        } catch (Exception e) {
            log.error("Failed to acknowledge batch {} to master", batchId, e);
            throw e;
        }
    }
    
    /**
     * 发送心跳
     */
    public void sendHeartbeat() {
        if (channel == null || channel.isShutdown()) {
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            ReplicationServiceProto.HeartbeatRequest request = ReplicationServiceProto.HeartbeatRequest.newBuilder()
                    .setSlaveNodeId(slaveNodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            ReplicationServiceProto.HeartbeatResponse response = blockingStub.heartbeat(request);
            
            if (response.getSuccess()) {
                log.debug("Heartbeat sent to master, master timestamp: {}", response.getMasterTimestamp());
            } else {
                log.warn("Heartbeat failed to master");
            }
            
        } catch (Exception e) {
            log.error("Failed to send heartbeat to master", e);
            throw e;
        }
    }
    
    /**
     * 注册从节点
     */
    public void registerSlave(String slaveHost, int slavePort, long lastSequence) {
        if (channel == null || channel.isShutdown()) {
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            ReplicationServiceProto.RegisterSlaveRequest request = ReplicationServiceProto.RegisterSlaveRequest.newBuilder()
                    .setSlaveNodeId(slaveNodeId)
                    .setSlaveHost(slaveHost)
                    .setSlavePort(slavePort)
                    .setLastSequence(lastSequence)
                    .build();
            
            ReplicationServiceProto.RegisterSlaveResponse response = blockingStub.registerSlave(request);
            
            if (response.getSuccess()) {
                log.info("Successfully registered slave {} with master, start sequence: {}", 
                        slaveNodeId, response.getStartSequence());
            } else {
                log.error("Failed to register slave {} with master: {}", slaveNodeId, response.getErrorMessage());
            }
            
        } catch (Exception e) {
            log.error("Failed to register slave {} with master", slaveNodeId, e);
            throw e;
        }
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                log.info("Disconnected from master at {}:{}", host, port);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while disconnecting from master", e);
            }
        }
    }
    
    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return channel != null && !channel.isShutdown();
    }
    
    /**
     * 获取从节点ID
     */
    public String getSlaveNodeId() {
        return slaveNodeId;
    }
}
