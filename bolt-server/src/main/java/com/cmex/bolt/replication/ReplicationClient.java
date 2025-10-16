package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.recovery.SnapshotReader;
import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
    private final int slaveId;
    private final String masterHost;
    private final int masterReplicationPort;
    private final BoltConfig config;

    private StreamObserver<ReplicationRequest> requestObserver;
    private final AtomicBoolean isRegistered = new AtomicBoolean(false);
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    
    // Journal文件处理相关
    private Path currentJournalFile;
    private boolean isJournalReplayInProgress = false;

    public ReplicationClient(int slaveId, String masterHost, int masterReplicationPort, BoltConfig config) {
        this.slaveId = slaveId;
        this.masterHost = masterHost;
        this.masterReplicationPort = masterReplicationPort;
        this.config = config;

        // 创建gRPC通道
        this.channel = ManagedChannelBuilder.forAddress(masterHost, masterReplicationPort)
                .usePlaintext()
                .build();

        this.stub = ReplicationServiceGrpc.newStub(channel);

        System.out.println("Created ReplicationClient for slave " + slaveId + " connecting to " + masterHost + ":" + masterReplicationPort);
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
                    isRegistered.set(false);
                }

                @Override
                public void onCompleted() {
                    System.out.println("Replication stream completed");
                    isRegistered.set(false);
                }
            };

            // 创建请求观察者
            requestObserver = stub.replicationStream(responseObserver);

            // 发送注册消息（必须是第一个消息）
            if (sendRegisterMessage()) {
                connectionLatch.countDown();
                System.out.println("ReplicationClient started successfully for slave " + slaveId);
                return true;
            } else {
                System.err.println("Failed to register slave " + slaveId);
                return false;
            }

        } catch (Exception e) {
            System.err.println("Failed to start ReplicationClient for slave " + slaveId + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * 发送注册消息
     */
    private boolean sendRegisterMessage() {
        try {
            RegisterMessage registerMessage = RegisterMessage.newBuilder()
                    .setSlaveId(slaveId)
                    .build();

            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setRegister(registerMessage)
                    .build();

            requestObserver.onNext(request);
            System.out.println("Sent registration message for slave " + slaveId);
            return true;

        } catch (Exception e) {
            System.err.println("Failed to send registration message for slave " + slaveId + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * 发送心跳消息
     */
    public boolean sendHeartbeat() {
        if (!isRegistered.get()) {
            System.out.println("Cannot send heartbeat: not connected or not registered");
            return false;
        }

        try {
            HeartbeatMessage heartbeatMessage = HeartbeatMessage.newBuilder()
                    .setSlaveId(slaveId)
                    .setTimestamp(System.currentTimeMillis())
                    .putStatus("status", "active")
                    .putStatus("lastHeartbeat", String.valueOf(lastHeartbeatTime.get()))
                    .build();

            ReplicationRequest request = ReplicationRequest.newBuilder()
                    .setHeartbeat(heartbeatMessage)
                    .build();

            requestObserver.onNext(request);
            lastHeartbeatTime.set(System.currentTimeMillis());

            System.out.println("Sent heartbeat for slave " + slaveId);
            return true;

        } catch (Exception e) {
            System.err.println("Failed to send heartbeat for slave " + slaveId + ": " + e.getMessage());
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
            case JOURNAL -> handleJournalReplayResponse(response.getJournal());
            default -> System.out.println("Unknown response type: " + response.getMessageCase());
        }
    }

    /**
     * 处理注册响应
     */
    private void handleRegisterResponse(RegisterResponse registerResponse) {
        if (registerResponse.getSuccess()) {
            isRegistered.set(true);
            System.out.println("Registration successful for slave " + slaveId);

            if (!registerResponse.getSnapshot().isEmpty()) {
                System.out.println("Received snapshot data: " + registerResponse.getSnapshot().size() + " bytes");
                // 处理快照数据
                if (config != null) {
                    SnapshotReader reader = new SnapshotReader(config.boltHome());
                    reader.extractSnapshot(registerResponse.getSnapshot().toByteArray());
                } else {
                    System.err.println("BoltConfig is null, cannot extract snapshot");
                }
            }
        } else {
            System.err.println("Registration failed for slave " + slaveId + ": " + registerResponse.getMessage());
            isRegistered.set(false);
        }
    }

    /**
     * 处理心跳响应
     */
    private void handleHeartbeatResponse(HeartbeatResponse heartbeatResponse) {
        if (heartbeatResponse.getSuccess()) {
            System.out.println("Heartbeat response received for slave " + slaveId);
        } else {
            System.out.println("Heartbeat failed for slave " + slaveId);
        }
    }


    /**
     * 处理Journal重放响应
     */
    private void handleJournalReplayResponse(JournalReplayMessage journal) {
        try {
            System.out.println("Received journal data: " + journal.getJournalData().size() + 
                    " bytes, isLast=" + journal.getIsLastChunk());
            
            // 如果是第一块数据，创建新的journal文件
            if (!isJournalReplayInProgress) {
                initializeJournalFile();
                isJournalReplayInProgress = true;
            }
            
            // 将数据追加到journal文件
            if (currentJournalFile != null) {
                Files.write(currentJournalFile, journal.getJournalData().toByteArray(), 
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                
                System.out.println("Appended journal data to file: " + currentJournalFile);
            }
            
            // 如果是最后一块，完成journal重放
            if (journal.getIsLastChunk()) {
                completeJournalReplay();
            }
            
        } catch (IOException e) {
            System.err.println("Failed to write journal data: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error processing journal replay: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 初始化Journal文件
     */
    private void initializeJournalFile() {
        try {
            if (config == null) {
                System.err.println("BoltConfig is null, cannot initialize journal file");
                return;
            }
            
            // 创建journal目录
            Path journalDir = Path.of(config.boltHome(), "journal");
            Files.createDirectories(journalDir);
            
            // 生成journal文件名
            String journalFileName = "journal.json";
            currentJournalFile = journalDir.resolve(journalFileName);
            
            // 创建空文件
            Files.deleteIfExists(currentJournalFile);
            Files.createFile(currentJournalFile);
            
            System.out.println("Initialized journal file: " + currentJournalFile);
            
        } catch (IOException e) {
            System.err.println("Failed to initialize journal file: " + e.getMessage());
            e.printStackTrace();
            currentJournalFile = null;
        }
    }
    
    /**
     * 完成Journal重放
     */
    private void completeJournalReplay() {
        try {
            if (currentJournalFile != null) {
                System.out.println("Journal replay completed. File saved: " + currentJournalFile);
                
                // 可以在这里添加额外的处理逻辑，比如：
                // 1. 验证文件完整性
                // 2. 更新配置
                // 3. 触发后续处理
                
                // 重置状态
                currentJournalFile = null;
                isJournalReplayInProgress = false;
                
                System.out.println("Journal replay process completed for slave " + slaveId);
            }
        } catch (Exception e) {
            System.err.println("Error completing journal replay: " + e.getMessage());
            e.printStackTrace();
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

            isRegistered.set(false);

            System.out.println("ReplicationClient stopped for slave " + slaveId);

        } catch (Exception e) {
            System.err.println("Error stopping ReplicationClient for slave " + slaveId + ": " + e.getMessage());
        }
    }

}