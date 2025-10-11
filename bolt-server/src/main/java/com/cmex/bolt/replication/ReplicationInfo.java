package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.ReplicationState;
import io.grpc.ManagedChannel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制信息类，包含slave节点的详细信息
 */
@Data
public class ReplicationInfo {

    private final int nodeId;
    private final String host;
    private final int port;
    private final int replicationPort;
    private final InetSocketAddress tcpAddress;
    private final InetSocketAddress replicationTcpAddress;
    
    private volatile ReplicationState state;
    private volatile LocalDateTime lastHeartbeat;
    private volatile LocalDateTime registeredTime;
    
    // 序列号信息
    private final AtomicLong lastSnapshotSequence = new AtomicLong(0);
    private final AtomicLong lastJournalSequence = new AtomicLong(0);
    private final AtomicLong lastRelaySequence = new AtomicLong(0);
    
    // 缓冲信息
    private volatile long firstReplicationId = -1;
    private volatile int bufferSize = 0;
    private volatile boolean canPublishRelay = false;
    
    // 连接状态
    private volatile boolean isConnected = false;
    private volatile String errorMessage = null;
    
    // gRPC 连接和 stub
    private volatile ManagedChannel slaveChannel;
    private volatile ReplicationSlaveServiceGrpc.ReplicationSlaveServiceStub slaveAsyncStub;

    @Builder
    public ReplicationInfo(int nodeId, String host, int port, int replicationPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.replicationPort = replicationPort;
        this.tcpAddress = new InetSocketAddress(host, port);
        this.replicationTcpAddress = new InetSocketAddress(host, replicationPort);
        this.state = ReplicationState.INITIAL;
        this.registeredTime = LocalDateTime.now();
        this.lastHeartbeat = LocalDateTime.now();
    }

    public void setLastRelaySequence(long sequence) {
        this.lastRelaySequence.set(sequence);
    }

    public void updateHeartbeat() {
        this.lastHeartbeat = LocalDateTime.now();
    }

    public void setLastSnapshotSequence(long sequence) {
        this.lastSnapshotSequence.set(sequence);
    }

    // 状态检查方法
    public boolean isReady() {
        return state == ReplicationState.READY && isConnected;
    }

    public boolean isInError() {
        return state == ReplicationState.ERROR;
    }


}
