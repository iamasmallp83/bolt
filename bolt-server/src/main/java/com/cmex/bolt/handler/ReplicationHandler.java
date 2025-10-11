package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.MasterServer;
import com.cmex.bolt.replication.ReplicationManager;
import com.cmex.bolt.replication.ReplicationProto.*;
import com.google.protobuf.ByteString;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.extern.slf4j.Slf4j;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 主节点专用：按顺序批处理发送复制请求
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private final BoltConfig config;
    private final ReplicationManager replicationManager;

    public ReplicationHandler(BoltConfig config) {
        this.config = config;
        this.replicationManager = new ReplicationManager(config);
    }
    
    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理中继事件
        if (!wrapper.isBusinessEvent()) {
            return;
        }

        // 检查是否有就绪的节点需要同步
        int readyNodeCount = replicationManager.getReadyNodeCount();
        if (readyNodeCount == 0) {
            return;
        }

        try {
            // 创建中继消息
            BatchRelayMessage relayMessage = createRelayMessage(wrapper, sequence);
            
            // 通过ReplicationManager发送到所有就绪的节点
            replicationManager.sendRelayMessage(relayMessage);
            
            log.debug("Replicated relay message sequence {} to {} nodes", sequence, readyNodeCount);
            
        } catch (Exception e) {
            log.error("Failed to replicate relay message sequence {}: {}", sequence, e.getMessage());
        }
        wrapper.getBuffer().resetReaderIndex();
    }

    /**
     * 创建中继消息
     */
    private BatchRelayMessage createRelayMessage(NexusWrapper wrapper, long sequence) {
        // 创建包含完整元数据的消息数据
        RelayMessageData messageData = RelayMessageData.newBuilder()
                .setId(wrapper.getId())
                .setPartition(wrapper.getPartition())
                .setEventType(wrapper.getEventType().getValue())
                .setData(ByteString.copyFrom(wrapper.cloneBuffer()))
                .build();

        return BatchRelayMessage.newBuilder()
                .setSequence(sequence)
                .setSize(1)
                .setTimestamp(System.currentTimeMillis())
                .addMessages(messageData)
                .build();
    }

    @Override
    public void onStart() {
        log.info("ReplicationHandler started");
        // 启动ReplicationManager
        replicationManager.start();
    }

    @Override
    public void onShutdown() {
        log.info("ReplicationHandler shutdown");
        // 停止ReplicationManager
        replicationManager.stop();
    }
}