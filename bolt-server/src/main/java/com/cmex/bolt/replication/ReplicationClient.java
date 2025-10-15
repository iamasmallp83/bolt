package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制服务客户端（支持双向通信）
 */
public class ReplicationClient {
    
    private final ManagedChannel channel;
    private final ReplicationServiceGrpc.ReplicationServiceStub stub;
    private final int nodeId;
    private final String host;
    private final int replicationPort;
    
    private StreamObserver<ReplicationRequest> requestObserver;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isRegistered = new AtomicBoolean(false);
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    
    public ReplicationClient(String targetHost, int targetPort, int nodeId, String host, int replicationPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.replicationPort = replicationPort;
        
        // 创建gRPC通道
        this.channel = ManagedChannelBuilder.forAddress(targetHost, targetPort)
                .usePlaintext()
                .build();
        
        this.stub = ReplicationServiceGrpc.newStub(channel);
        
        System.out.println("Created ReplicationClient for node " + nodeId + " connecting to " + targetHost + ":" + targetPort);
    }
    
    /**
     * 启动客户端连接
     */
    public boolean start() {
        try {
            // 创建响应观察者
            StreamObserver<ReplicationResponse> responseObserver = new StreamObserver<ReplicationResponse>() {
                @Override
                public void onNext(ReplicationResponse response) {
                    handleResponse(response);
                }
                
                @Override
                public void onError(Throwable t) {
                    System.err.println("Replication stream error: " + t.getMessage());
                    isConnected.set(false);
                    isRegistered.set(false);
                }
                
                @Override
                public void onCompleted() {
                    System.out.println("Replication stream completed");
                    isConnected.set(false);
                    isRegistered.set(false);
                }
            };
            
            // 创建请求观察者
            requestObserver = stub.replicationStream(responseObserver);
            
            // 发送注册消息（必须是第一个消息）
            if (sendRegisterMessage()) {
                isConnected.set(true);
                connectionLatch.countDown();
                System.out.println("ReplicationClient started successfully for node " + nodeId);
                return true;
            } else {
                System.err.println("Failed to register node " + nodeId);
                return false;
            }
            
        } catch (Exception e) {
            System.err.println("Failed to start ReplicationClient for node " + nodeId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 发送注册消息
     */
    private boolean sendRegisterMessage() {
        try {
            RegisterMessage registerMessage = RegisterMessage.newBuilder()
                    .setNodeId(nodeId)
                    .setHost(host)
                    .setReplicationPort(replicationPort)
                    .build();
            
            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setRegister(registerMessage)
                    .build();
            
            requestObserver.onNext(request);
            System.out.println("Sent registration message for node " + nodeId);
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to send registration message for node " + nodeId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 发送心跳消息
     */
    public boolean sendHeartbeat() {
        if (!isConnected.get() || !isRegistered.get()) {
            System.out.println("Cannot send heartbeat: not connected or not registered");
            return false;
        }
        
        try {
            HeartbeatMessage heartbeatMessage = HeartbeatMessage.newBuilder()
                    .setNodeId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .putStatus("status", "active")
                    .putStatus("lastHeartbeat", String.valueOf(lastHeartbeatTime.get()))
                    .build();
            
            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setHeartbeat(heartbeatMessage)
                    .build();
            
            requestObserver.onNext(request);
            lastHeartbeatTime.set(System.currentTimeMillis());
            
            System.out.println("Sent heartbeat for node " + nodeId);
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to send heartbeat for node " + nodeId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 发送Journal重放消息
     */
    public boolean sendJournalReplay(byte[] journalData, boolean isLastChunk) {
        if (!isConnected.get() || !isRegistered.get()) {
            System.out.println("Cannot send journal replay: not connected or not registered");
            return false;
        }
        
        try {
            JournalReplayMessage journalMessage = JournalReplayMessage.newBuilder()
                    .setJournalData(com.google.protobuf.ByteString.copyFrom(journalData))
                    .setIsLastChunk(isLastChunk)
                    .build();
            
            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setJournal(journalMessage)
                    .build();
            
            requestObserver.onNext(request);
            
            System.out.println("Sent journal replay: " + journalData.length + " bytes, isLast=" + isLastChunk);
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to send journal replay for node " + nodeId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 发送中继消息
     */
    public boolean sendRelayMessage(BatchRelayMessage relayMessage) {
        if (!isConnected.get() || !isRegistered.get()) {
            System.out.println("Cannot send relay message: not connected or not registered");
            return false;
        }
        
        try {
            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setRelay(relayMessage)
                    .build();
            
            requestObserver.onNext(request);
            
            System.out.println("Sent relay message batch " + relayMessage.getSequence() + 
                    " with " + relayMessage.getSize() + " messages");
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to send relay message for node " + nodeId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * 处理响应消息
     */
    private void handleResponse(ReplicationResponse response) {
        switch (response.getMessageCase()) {
            case REGISTER -> handleRegisterResponse(response.getRegister());
            case HEARTBEAT -> handleHeartbeatResponse(response.getHeartbeat());
            default -> System.out.println("Unknown response type: " + response.getMessageCase());
        }
    }
    
    /**
     * 处理注册响应
     */
    private void handleRegisterResponse(RegisterResponse registerResponse) {
        if (registerResponse.getSuccess()) {
            isRegistered.set(true);
            System.out.println("Registration successful for node " + nodeId);
            
            if (registerResponse.getSnapshot() != null && !registerResponse.getSnapshot().isEmpty()) {
                System.out.println("Received snapshot data: " + registerResponse.getSnapshot().size() + " bytes");
                // 这里可以处理快照数据
            }
        } else {
            System.err.println("Registration failed for node " + nodeId + ": " + registerResponse.getMessage());
            isRegistered.set(false);
        }
    }
    
    /**
     * 处理心跳响应
     */
    private void handleHeartbeatResponse(HeartbeatResponse heartbeatResponse) {
        if (heartbeatResponse.getSuccess()) {
            System.out.println("Heartbeat response received for node " + nodeId);
        } else {
            System.out.println("Heartbeat failed for node " + nodeId);
        }
    }
    
    /**
     * 等待连接建立
     */
    public boolean waitForConnection(long timeout, TimeUnit unit) {
        try {
            return connectionLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for connection: " + e.getMessage());
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return isConnected.get();
    }
    
    /**
     * 检查是否已注册
     */
    public boolean isRegistered() {
        return isRegistered.get();
    }
    
    /**
     * 停止客户端
     */
    public void stop() {
        try {
            if (requestObserver != null) {
                requestObserver.onCompleted();
            }
            
            if (channel != null && !channel.isShutdown()) {
                channel.shutdown();
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            }
            
            isConnected.set(false);
            isRegistered.set(false);
            
            System.out.println("ReplicationClient stopped for node " + nodeId);
            
        } catch (Exception e) {
            System.err.println("Error stopping ReplicationClient for node " + nodeId + ": " + e.getMessage());
        }
    }
    
    /**
     * 获取节点ID
     */
    public int getNodeId() {
        return nodeId;
    }
}