package com.cmex.bolt.replication;

import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 从节点复制服务实现
 */
@Slf4j
public class SlaveReplicationServiceImpl extends SlaveReplicationServiceGrpc.SlaveReplicationServiceImplBase {
    
    private final SlaveReplicationManager slaveReplicationManager;
    private final AtomicLong lastRelaySequence = new AtomicLong(0);
    private final AtomicLong lastSnapshotSequence = new AtomicLong(0);
    private final AtomicLong lastJournalSequence = new AtomicLong(0);
    
    public SlaveReplicationServiceImpl(SlaveReplicationManager slaveReplicationManager) {
        this.slaveReplicationManager = slaveReplicationManager;
    }
    
    @Override
    public StreamObserver<BatchRelayMessage> sendRelay(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<BatchRelayMessage>() {
            @Override
            public void onNext(BatchRelayMessage relayMessage) {
                try {
                    log.debug("Received relay message batch {} with {} messages", 
                            relayMessage.getSequence(), relayMessage.getSize());
                    
                    // 处理中继消息
                    boolean success = slaveReplicationManager.processRelayMessage(relayMessage);
                    
                    // 更新最后中继序列号
                    lastRelaySequence.set(relayMessage.getSequence());
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(relayMessage.getSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(success)
                            .build();
                    
                    responseObserver.onNext(response);
                    
                } catch (Exception e) {
                    log.error("Failed to process relay message batch {}", relayMessage.getSequence(), e);
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(relayMessage.getSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(false)
                            .setErrorMessage(e.getMessage())
                            .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Relay message stream error", t);
            }
            
            @Override
            public void onCompleted() {
                log.info("Relay message stream completed");
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public StreamObserver<SnapshotReplayMessage> sendSnapshot(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<SnapshotReplayMessage>() {
            @Override
            public void onNext(SnapshotReplayMessage snapshotMessage) {
                try {
                    log.info("Received snapshot data: partition={}, type={}, isLast={}", 
                            snapshotMessage.getPartition(), 
                            snapshotMessage.getDataType(),
                            snapshotMessage.getIsLastPartition());
                    
                    // 处理快照数据
                    slaveReplicationManager.processSnapshotData(snapshotMessage);
                    
                    // 更新最后快照序列号
                    lastSnapshotSequence.set(snapshotMessage.getSnapshotTimestamp());
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(snapshotMessage.getSnapshotTimestamp())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(true)
                            .build();
                    
                    responseObserver.onNext(response);
                    
                    // 如果是最后一个分区，更新状态
                    if (snapshotMessage.getIsLastPartition()) {
                        slaveReplicationManager.updateState(ReplicationState.RELAY_BUFFERING);
                        log.info("Snapshot sync completed, switching to relay buffering state");
                    }
                    
                } catch (Exception e) {
                    log.error("Failed to process snapshot data", e);
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(snapshotMessage.getSnapshotTimestamp())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(false)
                            .setErrorMessage(e.getMessage())
                            .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Snapshot stream error", t);
            }
            
            @Override
            public void onCompleted() {
                log.info("Snapshot stream completed");
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public StreamObserver<JournalReplayMessage> sendJournal(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<JournalReplayMessage>() {
            @Override
            public void onNext(JournalReplayMessage journalMessage) {
                try {
                    log.debug("Received journal replay: sequence={}, isLast={}", 
                            journalMessage.getSequence(), journalMessage.getIsLastChunk());
                    
                    // 处理Journal重放
                    slaveReplicationManager.processJournalData(journalMessage);
                    
                    // 更新最后Journal序列号
                    lastJournalSequence.set(journalMessage.getSequence());
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(journalMessage.getSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(true)
                            .build();
                    
                    responseObserver.onNext(response);
                    
                    // 如果是最后一块，发布缓冲的业务消息
                    if (journalMessage.getIsLastChunk()) {
                        slaveReplicationManager.publishBufferedRelayMessages();
                        log.info("Journal sync completed, published buffered relay messages");
                    }
                    
                } catch (Exception e) {
                    log.error("Failed to process journal replay", e);
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(journalMessage.getSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(false)
                            .setErrorMessage(e.getMessage())
                            .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Journal replay stream error", t);
            }
            
            @Override
            public void onCompleted() {
                log.info("Journal replay stream completed");
                responseObserver.onCompleted();
            }
        };
    }
    
    /**
     * 获取最后中继序列号
     */
    public long getLastRelaySequence() {
        return lastRelaySequence.get();
    }
    
    /**
     * 获取最后快照序列号
     */
    public long getLastSnapshotSequence() {
        return lastSnapshotSequence.get();
    }
    
    /**
     * 获取最后Journal序列号
     */
    public long getLastJournalSequence() {
        return lastJournalSequence.get();
    }
}
