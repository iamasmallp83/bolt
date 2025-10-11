package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.recovery.SnapshotReader;
import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 主节点复制服务实现
 */
@Slf4j
public class MasterReplicationServiceImpl extends MasterReplicationServiceGrpc.MasterReplicationServiceImplBase {
    private final BoltConfig config;
    private final MasterServer masterServer;
    private final ConcurrentMap<Integer, StreamObserver<HeartbeatResponse>> heartbeatObservers = new ConcurrentHashMap<>();

    public MasterReplicationServiceImpl(BoltConfig config, MasterServer masterServer) {
        this.config = config;
        this.masterServer = masterServer;
    }

    @Override
    public void registerSlave(RegisterMessage request, StreamObserver<RegisterResponse> responseObserver) {
        try {
            log.info("Received slave registration request from node {} at {}:{}",
                    request.getNodeId(), request.getHost(), request.getPort());

            // 分配节点ID（如果请求中没有指定）
            int assignedNodeId = request.getNodeId();

            // 注册到MasterServer
            boolean success = masterServer.registerNode(request);
            RegisterResponse response;
            if (success) {
                // 尝试获取并打包快照数据
                byte[] snapshotData = null;
                try {
                    SnapshotReader snapshotReader = new SnapshotReader(config);
                    snapshotData = snapshotReader.packSnapshot();
                    if (snapshotData != null) {
                        log.info("Packed snapshot for slave {}: {} bytes", assignedNodeId, snapshotData.length);
                    } else {
                        log.info("No snapshot available for slave {}", assignedNodeId);
                    }
                } catch (Exception e) {
                    log.error("Failed to process snapshot for slave {}", assignedNodeId, e);
                }
                
                RegisterResponse.Builder responseBuilder = RegisterResponse.newBuilder()
                        .setSuccess(success)
                        .setAssignedNodeId(assignedNodeId)
                        .setServerTime(System.currentTimeMillis());
                
                // 如果有快照数据，添加到响应中
                if (snapshotData != null) {
                    responseBuilder.setSnapshot(com.google.protobuf.ByteString.copyFrom(snapshotData));
                }
                
                response = responseBuilder.build();
            } else {
                response = RegisterResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Registration failed")
                        .setAssignedNodeId(0)
                        .setServerTime(System.currentTimeMillis())
                        .build();
            }

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
                    masterServer.updateHeartbeat(nodeId);

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
    public void reportLatestReplication(LatestReplicationMessage request, StreamObserver<ConfirmationMessage> responseObserver) {
        try {
            log.info("Received latest replication report from node {}: replication_id={}",
                    request.getNodeId(), request.getReplicationId());

            // 处理最新复制ID报告
            masterServer.handleLatestReplicationReport(request);

            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getReplicationId())
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Failed to process latest replication report from node {}", request.getNodeId(), e);

            ConfirmationMessage response = ConfirmationMessage.newBuilder()
                    .setNodeId(request.getNodeId())
                    .setSequence(request.getReplicationId())
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

}
