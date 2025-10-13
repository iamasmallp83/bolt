package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.ReplicationState;
import io.grpc.ManagedChannel;
import lombok.Builder;
import lombok.Data;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;

/**
 * 复制信息类，包含slave节点的详细信息
 */
@Data
public class SlaveInfo {

    private final int nodeId;
    private final String host;
    private final int replicationPort;
    private final InetSocketAddress tcpAddress;
    private final InetSocketAddress replicationTcpAddress;

    private volatile ReplicationState state;
    private volatile LocalDateTime lastHeartbeat;
    private volatile LocalDateTime registeredTime;

    // 序列号信息
    private volatile long maxRequestId = -1;

    // 缓冲信息
    private volatile int bufferSize = 0;
    private volatile boolean canPublishRelay = false;

    // 连接状态
    private volatile boolean isConnected = false;
    private volatile String errorMessage = null;

    // gRPC 连接和 stub
    private volatile ManagedChannel slaveChannel;
    private volatile ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub slaveAsyncStub;

    @Builder
    public SlaveInfo(int nodeId, String host, int replicationPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.replicationPort = replicationPort;
        this.tcpAddress = new InetSocketAddress(host, replicationPort);
        this.replicationTcpAddress = new InetSocketAddress(host, replicationPort);
        this.state = ReplicationState.INITIAL;
        this.registeredTime = LocalDateTime.now();
        this.lastHeartbeat = LocalDateTime.now();
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = LocalDateTime.now();
    }

    // 状态检查方法
    public boolean isReady() {
        return state == ReplicationState.READY && isConnected;
    }

    public boolean isInError() {
        return state == ReplicationState.ERROR;
    }


}
