package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.recovery.SnapshotReader;
import com.cmex.bolt.replication.ReplicationProto.*;
import com.lmax.disruptor.RingBuffer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 统一的复制服务实现（支持双向通信）
 */
public class ReplicationServer extends ReplicationServiceGrpc.ReplicationServiceImplBase {

    private final BoltConfig config;

    private final RingBuffer<NexusWrapper> sequencerRingBuffer;

    // 存储活跃的连接
    private final ConcurrentMap<Integer, StreamObserver<ReplicationResponse>> registeredSlaves = new ConcurrentHashMap<>();

    public ReplicationServer(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.config = config;
        this.sequencerRingBuffer = sequencerRingBuffer;
        try {
            Server server = ServerBuilder.forPort(config.masterReplicationPort())
                    .addService(this)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamObserver<ReplicationRequest> replicationStream(StreamObserver<ReplicationResponse> responseObserver) {
        return new StreamObserver<ReplicationRequest>() {
            private int nodeId = 0;

            @Override
            public void onNext(ReplicationRequest request) {
                try {
                    // 根据消息类型处理
                    switch (request.getMessageCase()) {
                        case REGISTER -> nodeId = handleRegisterMessage(request.getRegister(), responseObserver);
                        case HEARTBEAT -> handleHeartbeatMessage(request.getHeartbeat(), responseObserver);
                        default -> System.out.println("Unknown message type received: " + request.getMessageCase());
                    }

                } catch (Exception e) {
                    System.err.println("Failed to process replication request: " + e.getMessage());
                    sendErrorResponse(responseObserver, "Processing failed: " + e.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                cleanupConnection();
            }

            @Override
            public void onCompleted() {
                cleanupConnection();
                responseObserver.onCompleted();
            }

            private void cleanupConnection() {
                registeredSlaves.remove(nodeId);
            }
        };
    }

    /**
     * 处理注册消息
     */
    private int handleRegisterMessage(RegisterMessage registerMessage, StreamObserver<ReplicationResponse> responseObserver) {
        int slaveId = registerMessage.getSlaveId();
        try {
            // 检查是否已经注册
            if (registeredSlaves.containsKey(slaveId)) {
                System.out.println("Slave " + slaveId + " is already registered");
                sendRegisterResponse(responseObserver, false, "Slave already registered", 0, null);
                return -1;
            }
            // 尝试获取快照数据
            byte[] snapshotData = null;
            try {
                if (config != null) {
                    SnapshotReader snapshotReader = new SnapshotReader(config.boltHome());
                    snapshotData = snapshotReader.packSnapshot();
                    if (snapshotData != null) {
                        System.out.println("Packed snapshot for slave " + slaveId + ": " + snapshotData.length + " bytes");
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to process snapshot for slave " + slaveId + ": " + e.getMessage());
            }

            // 标记为已注册
            registeredSlaves.put(slaveId, responseObserver);

            sendRegisterResponse(responseObserver, true, "Registration successful", slaveId, snapshotData);
            System.out.println("Slave " + slaveId + " registered successfully");
            sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(-1);
                wrapper.setPartition(-1);
                wrapper.setEventType(NexusWrapper.EventType.SLAVE_JOINED);
                wrapper.getBuffer().writeInt(slaveId);
            });
            return slaveId;
        } catch (Exception e) {
            System.err.println("Failed to handle registration for slave " + slaveId + ": " + e.getMessage());
            sendRegisterResponse(responseObserver, false, "Registration failed: " + e.getMessage(), 0, null);
            return -1;
        }
    }

    /**
     * 处理心跳消息
     */
    private void handleHeartbeatMessage(HeartbeatMessage heartbeatMessage, StreamObserver<ReplicationResponse> responseObserver) {
        try {
            int slaveId = heartbeatMessage.getSlaveId();

            // 检查是否已注册
            if (!registeredSlaves.containsKey(slaveId)) {
                System.out.println("Received heartbeat from unregistered slave " + slaveId);
                sendHeartbeatResponse(responseObserver, false, "slave not registered");
                return;
            }

            // 发送心跳响应
            sendHeartbeatResponse(responseObserver, true, "Heartbeat received");

            System.out.println("Processed heartbeat from node " + slaveId);

        } catch (Exception e) {
            System.err.println("Failed to handle heartbeat from node " + heartbeatMessage.getSlaveId() + ": " + e.getMessage());
            sendHeartbeatResponse(responseObserver, false, "Heartbeat processing failed");
        }
    }

    /**
     * 发送注册响应
     */
    private void sendRegisterResponse(StreamObserver<ReplicationResponse> responseObserver,
                                      boolean success, String message, int nodeId, byte[] snapshot) {
        RegisterResponse.Builder builder = RegisterResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .setServerTime(System.currentTimeMillis());

        if (snapshot != null) {
            builder.setSnapshot(com.google.protobuf.ByteString.copyFrom(snapshot));
        }

        ReplicationResponse response = ReplicationResponse.newBuilder()
                .setRegister(builder.build())
                .build();

        responseObserver.onNext(response);
    }

    /**
     * 发送心跳响应
     */
    private void sendHeartbeatResponse(StreamObserver<ReplicationResponse> responseObserver,
                                       boolean success, String message) {
        HeartbeatResponse heartbeatResponse = HeartbeatResponse.newBuilder()
                .setSuccess(success)
                .setServerTime(System.currentTimeMillis())
                .build();

        ReplicationResponse response = ReplicationResponse.newBuilder()
                .setHeartbeat(heartbeatResponse)
                .build();

        responseObserver.onNext(response);
    }

    /**
     * 发送错误响应
     */
    private void sendErrorResponse(StreamObserver<ReplicationResponse> responseObserver, String errorMessage) {
        // 由于当前proto没有错误响应类型，我们使用心跳响应来表示错误
        HeartbeatResponse errorResponse = HeartbeatResponse.newBuilder()
                .setSuccess(false)
                .setServerTime(System.currentTimeMillis())
                .build();

        ReplicationResponse response = ReplicationResponse.newBuilder()
                .setHeartbeat(errorResponse)
                .build();

        responseObserver.onNext(response);
    }

    /**
     * 发送Journal数据到指定的slave
     */
    public void sendJournalToSlave(int slaveId, byte[] journalData, boolean isLastChunk) {
        StreamObserver<ReplicationResponse> slaveObserver = registeredSlaves.get(slaveId);
        if (slaveObserver == null) {
            System.err.println("Slave " + slaveId + " is not registered or disconnected");
            return;
        }
        
        try {
            JournalReplayMessage journalMessage = JournalReplayMessage.newBuilder()
                    .setIsLastChunk(isLastChunk)
                    .setJournalData(com.google.protobuf.ByteString.copyFrom(journalData))
                    .build();
            
            ReplicationResponse response = ReplicationResponse.newBuilder()
                    .setJournal(journalMessage)
                    .build();
            
            slaveObserver.onNext(response);
            System.out.println("Sent journal data to slave " + slaveId + ": " + journalData.length + " bytes, isLast=" + isLastChunk);
            
        } catch (Exception e) {
            System.err.println("Failed to send journal data to slave " + slaveId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 发送Journal文件到指定的slave
     */
    public void sendJournalFileToSlave(int slaveId, java.nio.file.Path journalPath) {
        try {
            if (!java.nio.file.Files.exists(journalPath)) {
                System.err.println("Journal file does not exist: " + journalPath);
                return;
            }
            
            byte[] journalData = java.nio.file.Files.readAllBytes(journalPath);
            sendJournalToSlave(slaveId, journalData, true); // 文件传输完成，标记为最后一块
            
        } catch (Exception e) {
            System.err.println("Failed to read journal file " + journalPath + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 获取活跃节点数量
     */
    public StreamObserver<ReplicationResponse> getSlave(int salveId) {
        return registeredSlaves.get(salveId);
    }
    
    /**
     * 获取已注册的slave数量
     */
    public int getRegisteredSlaveCount() {
        return registeredSlaves.size();
    }
    
    /**
     * 检查slave是否已注册
     */
    public boolean isSlaveRegistered(int slaveId) {
        return registeredSlaves.containsKey(slaveId);
    }
}