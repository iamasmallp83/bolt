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
    private final AtomicLong lastBusinessSequence = new AtomicLong(0);
    private final AtomicLong lastSnapshotSequence = new AtomicLong(0);
    private final AtomicLong lastJournalSequence = new AtomicLong(0);
    
    public SlaveReplicationServiceImpl(SlaveReplicationManager slaveReplicationManager) {
        this.slaveReplicationManager = slaveReplicationManager;
    }
    
    @Override
    public StreamObserver<BatchBusinessMessage> sendBusiness(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<BatchBusinessMessage>() {
            @Override
            public void onNext(BatchBusinessMessage businessMessage) {
                try {
                    log.debug("Received business message batch {} with {} messages", 
                            businessMessage.getBatchId(), businessMessage.getBatchSize());
                    
                    // 处理业务消息
                    boolean success = slaveReplicationManager.processBusinessMessage(businessMessage);
                    
                    // 更新最后业务序列号
                    lastBusinessSequence.set(businessMessage.getEndSequence());
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(businessMessage.getEndSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(success)
                            .build();
                    
                    responseObserver.onNext(response);
                    
                } catch (Exception e) {
                    log.error("Failed to process business message batch {}", businessMessage.getBatchId(), e);
                    
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveReplicationManager.getAssignedNodeId())
                            .setSequence(businessMessage.getEndSequence())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(false)
                            .setErrorMessage(e.getMessage())
                            .build();
                    
                    responseObserver.onNext(response);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                log.error("Business message stream error", t);
            }
            
            @Override
            public void onCompleted() {
                log.info("Business message stream completed");
                responseObserver.onCompleted();
            }
        };
    }
    
    @Override
    public StreamObserver<SnapshotDataMessage> sendSnapshot(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<SnapshotDataMessage>() {
            @Override
            public void onNext(SnapshotDataMessage snapshotMessage) {
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
                        slaveReplicationManager.updateState(ReplicationState.BUSINESS_BUFFERING);
                        log.info("Snapshot sync completed, switching to business buffering state");
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
                        slaveReplicationManager.publishBufferedBusinessMessages();
                        log.info("Journal sync completed, published buffered business messages");
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
     * 获取最后业务序列号
     */
    public long getLastBusinessSequence() {
        return lastBusinessSequence.get();
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
