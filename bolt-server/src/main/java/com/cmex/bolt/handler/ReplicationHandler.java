package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationServer;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;


/**
 * 复制处理器 - 负责将事件复制到从节点
 * 主节点专用：按顺序批处理发送复制请求
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private final BoltConfig config;
    private final ReplicationServer replicationServer;

    public ReplicationHandler(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.config = config;
        this.replicationServer = new ReplicationServer(config, sequencerRingBuffer);
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        if (wrapper.isSlaveJoined()) {
            handleSlaveJoined(wrapper);
            return;
        }
        if (!wrapper.isBusinessEvent()) {
            return;
        }

        replicationServer.relay(wrapper);

        wrapper.getBuffer().resetReaderIndex();
    }

    private void handleSlaveJoined(NexusWrapper wrapper) {
        Path currentJournalPath = Path.of(config.journalFilePath());

        if (!Files.exists(currentJournalPath)) {
            log.warn("Current journal file does not exist: {}", currentJournalPath);
            return;
        }

        // 生成带 replication 前缀的文件名
        int slaveId = wrapper.getBuffer().readInt();
        String replicationFilename = "replication_" + slaveId + "_" + currentJournalPath.getFileName().toString();
        Path journalDir = currentJournalPath.getParent();
        Path replicationJournalPath = journalDir.resolve(replicationFilename);
        // 发送到slave
        replicationServer.sendJournalFileToSlave(slaveId, replicationJournalPath);
    }

    @Override
    public void onStart() {

    }

    @Override
    public void onShutdown() {

    }
}