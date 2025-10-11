package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 主节点复制服务实现
 */
@Slf4j
public class MasterReplicationServiceImpl extends MasterReplicationServiceGrpc.MasterReplicationServiceImplBase {
    
    private final ReplicationManager replicationManager;
    private final ConcurrentMap<Integer, StreamObserver<HeartbeatResponse>> heartbeatObservers = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, StreamObserver<StateSyncMessage>> stateSyncObservers = new ConcurrentHashMap<>();
    private final AtomicInteger nextNodeId = new AtomicInteger(1);
    
    public MasterReplicationServiceImpl(ReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
    }
    
    @Override
    public void registerSlave(RegisterMessage request, StreamObserver<RegisterResponse> responseObserver) {
        try {
            log.info("Received slave registration request from node {} at {}:{}", 
                    request.getNodeId(), request.getHost(), request.getPort());
            
            // 分配节点ID（如果请求中没有指定）
            int assignedNodeId = request.getNodeId() > 0 ? request.getNodeId() : nextNodeId.getAndIncrement();
            
            // 创建注册消息用于ReplicationManager
            RegisterMessage registerMessage = RegisterMessage.newBuilder()
                    .setNodeId(assignedNodeId)
                    .setHost(request.getHost())
                    .setPort(request.getPort())
                    .setReplicationPort(request.getReplicationPort())
                    .putAllMetadata(request.getMetadataMap())
                    .build();
            
            // 注册到ReplicationManager
            boolean success = replicationManager.registerNode(registerMessage);
            
            RegisterResponse response = RegisterResponse.newBuilder()
                    .setSuccess(success)
                    .setMessage(success ? "Registration successful" : "Registration failed")
                    .setAssignedNodeId(assignedNodeId)
                    .setServerTime(System.currentTimeMillis())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
            log.info("Slave node {} registered successfully", assignedNodeId);
            
        } catch (Exception e) {
            log.error("Failed to register slave node", e);
            
            RegisterResponse response = RegisterResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Registration failed: " + e.getMessage())
                    .setAssignedNodeId(0)
                    .setServerTime(System.currentTimeMillis())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    @Override
    public StreamObserver<HeartbeatMessage> heartbeat(StreamObserver<HeartbeatResponse> responseObserver) {
        return new StreamObserver<HeartbeatMessage>() {
            private int nodeId = -1;
            
            @Override
            public void onNext(HeartbeatMessage heartbeatMessage) {
                try {
                    nodeId = heartbeatMessage.getNodeId();
                    
                    // 更新心跳
                    replicationManager.updateHeartbeat(nodeId);
                    
                    // 保存响应观察者
                    heartbeatObservers.put(nodeId, responseObserver);
                    
                    // 发送心跳响应
                    HeartbeatResponse response = HeartbeatResponse.newBuilder()
                            .setSuccess(true)
                            .setServerTime(System.currentTimeMillis())
                            .setNextHeartbeatInterval(30) // 30秒间隔
                            .build();
                    
                    responseObserver.onNext(response);
                    
                    log.debug("Processed heartbeat from node {}", nodeId);
                    
                } catch (Exception e) {
                    log.error("Failed to process heartbeat from node {}", nodeId, e);
                    
                    HeartbeatResponse response = HeartbeatResponse.newBuilder()
                            .setSuccess(false)
                            .setServerTime(System.currentTimeMillis())
                            .setNextHeartbeatInterval(30)
                            .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Heartbeat stream error for node {}", nodeId, t);
                heartbeatObservers.remove(nodeId);
            }
            
            @Override
            public void onCompleted() {
                log.info("Heartbeat stream completed for node {}", nodeId);
                heartbeatObservers.remove(nodeId);
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public StreamObserver<StateSyncMessage> syncState(StreamObserver<StateSyncMessage> responseObserver) {
        return new StreamObserver<StateSyncMessage>() {
            private int nodeId = -1;
            
            @Override
            public void onNext(StateSyncMessage stateSyncMessage) {
                try {
                    nodeId = stateSyncMessage.getNodeId();
                    
                    // 更新节点状态
                    replicationManager.updateNodeState(nodeId, stateSyncMessage.getCurrentState());
                    
                    // 保存响应观察者
                    stateSyncObservers.put(nodeId, responseObserver);
                    
                    // 发送状态同步响应
                    StateSyncMessage response = StateSyncMessage.newBuilder()
                            .setNodeId(nodeId)
                            .setCurrentState(stateSyncMessage.getCurrentState())
                            .setLastSequence(stateSyncMessage.getLastSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .build();
                    
                    responseObserver.onNext(response);
                    
                    log.debug("Processed state sync from node {}", nodeId);
                    
                } catch (Exception e) {
                    log.error("Failed to process state sync from node {}", nodeId, e);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("State sync stream error for node {}", nodeId, t);
                stateSyncObservers.remove(nodeId);
            }
            
            @Override
            public void onCompleted() {
                log.info("State sync stream completed for node {}", nodeId);
                stateSyncObservers.remove(nodeId);
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public void reportBufferFirstId(BufferFirstIdReportMessage request, StreamObserver<ConfirmationMessage> responseObserver) {
        try {
            log.info("Received buffer first ID report from node {}: ID={}, size={}", 
                    request.getNodeId(), request.getFirstBufferedId(), request.getBufferSize());
            
            // 处理缓冲ID报告
            replicationManager.handleBufferFirstIdReport(request);
            
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getFirstBufferedId())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to process buffer first ID report from node {}", request.getNodeId(), e);
            
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getFirstBufferedId())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    @Override
    public void confirmRelayPublish(RelayPublishConfirmMessage request, StreamObserver<ConfirmationMessage> responseObserver) {
        try {
            log.info("Received relay publish confirmation from node {}: sequence={}", 
                    request.getNodeId(), request.getPublishedSequence());
            
            // 更新节点状态为就绪
            replicationManager.updateNodeState(request.getNodeId(), ReplicationState.READY);
            
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getPublishedSequence())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(true)
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Failed to process relay publish confirmation from node {}", request.getNodeId(), e);
            
            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getPublishedSequence())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * 获取心跳观察者
     */
    public StreamObserver<HeartbeatResponse> getHeartbeatObserver(int nodeId) {
        return heartbeatObservers.get(nodeId);
    }
    
    /**
     * 获取状态同步观察者
     */
    public StreamObserver<StateSyncMessage> getStateSyncObserver(int nodeId) {
        return stateSyncObservers.get(nodeId);
    }
}
