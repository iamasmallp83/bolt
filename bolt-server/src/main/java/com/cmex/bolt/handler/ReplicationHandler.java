package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationManager;
import com.cmex.bolt.replication.ReplicationProto.BatchBusinessMessage;
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

    public ReplicationHandler(BoltConfig config, ReplicationManager replicationManager) {
        this.config = config;
        this.replicationManager = replicationManager;
    }
    
    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理业务事件
        if (!wrapper.isBusinessEvent()) {
            return;
        }

        // 检查是否有就绪的节点需要同步
        int readyNodeCount = replicationManager.getReadyNodeCount();
        if (readyNodeCount == 0) {
            return;
        }

        try {
            // 创建业务消息
            BatchBusinessMessage businessMessage = createBusinessMessage(wrapper, sequence);
            
            // 通过ReplicationManager发送到所有就绪的节点
            replicationManager.sendBusinessMessage(businessMessage);
            
            log.debug("Replicated business message sequence {} to {} nodes", sequence, readyNodeCount);
            
        } catch (Exception e) {
            log.error("Failed to replicate business message sequence {}: {}", sequence, e.getMessage());
        }
    }

    /**
     * 创建业务消息
     */
    private BatchBusinessMessage createBusinessMessage(NexusWrapper wrapper, long sequence) {
        return BatchBusinessMessage.newBuilder()
                .setBatchId(System.currentTimeMillis())
                .setBatchSize(1)
                .setStartSequence(sequence)
                .setEndSequence(sequence)
                .setTimestamp(System.currentTimeMillis())
                .addMessages(com.google.protobuf.ByteString.copyFrom(wrapper.getBufferCopy()))
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