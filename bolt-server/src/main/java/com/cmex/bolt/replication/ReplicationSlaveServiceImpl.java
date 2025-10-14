package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.handler.JournalReplayer;
import com.cmex.bolt.recovery.SnapshotRecovery;
import com.cmex.bolt.replication.ReplicationProto.*;
import com.lmax.disruptor.RingBuffer;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 从节点复制服务实现
 */
@Slf4j
public class ReplicationSlaveServiceImpl extends ReplicationSlaveServiceGrpc.ReplicationSlaveServiceImplBase {

    @Getter
    private final SlaveSyncManager slaveSyncManager;
    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    private final BoltConfig config;
    private final AtomicLong lastRelaySequence = new AtomicLong(0);

    public ReplicationSlaveServiceImpl(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.sequencerRingBuffer = sequencerRingBuffer;
        this.slaveSyncManager = new SlaveSyncManager(config, sequencerRingBuffer);
        this.config = config;
    }

    @Override
    public StreamObserver<BatchRelayMessage> sendRelay(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<BatchRelayMessage>() {
            private boolean hasResponded = false;

            @Override
            public void onNext(BatchRelayMessage relayMessage) {
                try {
                    log.debug("Received relay message batch {} with {} messages",
                            relayMessage.getSequence(), relayMessage.getSize());

                    // 处理中继消息
                    boolean success = slaveSyncManager.processRelayMessage(relayMessage);

                    // 更新最后中继序列号
                    lastRelaySequence.set(relayMessage.getSequence());

                    // 只在第一次收到消息时发送确认，或者处理失败时发送错误确认
                    if (!hasResponded) {
                        ConfirmationMessage response = ConfirmationMessage.newBuilder()
                                .setNodeId(slaveSyncManager.getAssignedNodeId())
                                .setSequence(relayMessage.getSequence())
                                .setTimestamp(System.currentTimeMillis())
                                .setSuccess(success)
                                .build();

                        responseObserver.onNext(response);
                        hasResponded = true;
                    }

                } catch (Exception e) {
                    log.error("Failed to process relay message batch {}", relayMessage.getSequence(), e);

                    // 如果还没有响应过，发送错误确认
                    if (!hasResponded) {
                        ConfirmationMessage response = ConfirmationMessage.newBuilder()
                                .setNodeId(slaveSyncManager.getAssignedNodeId())
                                .setSequence(relayMessage.getSequence())
                                .setTimestamp(System.currentTimeMillis())
                                .setSuccess(false)
                                .setErrorMessage(e.getMessage())
                                .build();

                        responseObserver.onNext(response);
                        hasResponded = true;
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Relay message stream error", t);
                if (!hasResponded) {
                    responseObserver.onError(t);
                }
            }

            @Override
            public void onCompleted() {
                log.info("Relay message stream completed");
                if (!hasResponded) {
                    // 如果流结束但还没有发送过响应，发送一个默认的成功确认
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveSyncManager.getAssignedNodeId())
                            .setSequence(lastRelaySequence.get())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(true)
                            .build();

                    responseObserver.onNext(response);
                }
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<JournalReplayMessage> sendJournal(StreamObserver<ConfirmationMessage> responseObserver) {
        return new StreamObserver<JournalReplayMessage>() {
            private boolean hasResponded = false;
            private boolean processingSuccessful = true;
            private String errorMessage = null;

            @Override
            public void onNext(JournalReplayMessage journalMessage) {
                try {
                    log.info("Received journal replay: dataSize={}, isLast={}",
                            journalMessage.getJournalData().size(), journalMessage.getIsLastChunk());

                    // 直接将journal data写入文件
                    writeJournalDataToFile(journalMessage);
                    JournalReplayer replayer = new JournalReplayer(config, sequencerRingBuffer);
                    replayer.replayFromJournal();
                    slaveSyncManager.updateState(ReplicationState.READY);

                    log.info("Successfully processed journal chunk: {} bytes",
                            journalMessage.getJournalData().size());

                } catch (Exception e) {
                    log.error("Failed to process journal replay", e);
                    processingSuccessful = false;
                    errorMessage = e.getMessage();
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Journal replay stream error", t);
                processingSuccessful = false;
                errorMessage = t.getMessage();

                if (!hasResponded) {
                    ConfirmationMessage response = ConfirmationMessage.newBuilder()
                            .setNodeId(slaveSyncManager.getAssignedNodeId())
                            .setSequence(System.currentTimeMillis())
                            .setTimestamp(System.currentTimeMillis())
                            .setSuccess(false)
                            .setErrorMessage(errorMessage)
                            .build();

                    responseObserver.onNext(response);
                    hasResponded = true;
                }
            }

            @Override
            public void onCompleted() {
                log.info("Journal replay stream completed, processing successful: {}", processingSuccessful);

                // 发送最终确认消息
                ConfirmationMessage response = ConfirmationMessage.newBuilder()
                        .setNodeId(slaveSyncManager.getAssignedNodeId())
                        .setSequence(System.currentTimeMillis())
                        .setTimestamp(System.currentTimeMillis())
                        .setSuccess(processingSuccessful)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                log.info("Sent journal replay confirmation: success={}", processingSuccessful);
            }
        };
    }

    /**
     * 将journal数据写入文件
     */
    private void writeJournalDataToFile(JournalReplayMessage journalMessage) throws IOException {
        // 获取journal文件路径
        Path journalPath = Paths.get(config.journalFilePath());

        // 确保目录存在
        Files.createDirectories(journalPath.getParent());

        // 根据isBinary判断写入方式
        if (config.isBinary()) {
            // 二进制格式：直接写入字节数据
            // 使用TRUNCATE_EXISTING确保覆盖现有文件，避免重复数据
            Files.write(journalPath, journalMessage.getJournalData().toByteArray(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Wrote {} bytes of binary journal data to {}",
                    journalMessage.getJournalData().size(), journalPath);
        } else {
            // JSON格式：将字节数据转换为字符串写入
            String journalData = new String(journalMessage.getJournalData().toByteArray());
            Files.write(journalPath, journalData.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Wrote {} characters of JSON journal data to {}",
                    journalData.length(), journalPath);
        }
    }

}
