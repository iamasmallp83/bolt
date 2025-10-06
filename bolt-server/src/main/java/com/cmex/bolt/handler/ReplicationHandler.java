package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 主节点专用：按顺序批处理发送复制请求
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private final BoltConfig config;
    private final ReplicationState replicationState;

    // TCP复制服务器引用（用于通过现有连接发送数据）
    private TcpReplicationServer tcpReplicationServer;

    // 批处理器
    private final ReplicationBatchProcessor batchProcessor;

    // 性能统计
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);

    public ReplicationHandler(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;

        // 创建批处理器
        this.batchProcessor = new ReplicationBatchProcessor(
            config.batchSize(),
            config.batchTimeout(),
            this::sendBatchToSlaves
        );

        log.info("ReplicationHandler initialized - replicationPort: {}, batchSize: {}, batchTimeout: {}",
                config.replicationPort(), config.batchSize(), config.batchTimeout());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理业务事件
        if (wrapper.isJournalEvent()) {
            return;
        }

        totalEventsProcessed.incrementAndGet();
        
        // 将事件添加到批处理器，按顺序处理
        batchProcessor.addEvent(wrapper, sequence, endOfBatch);
    }

    /**
     * 发送批处理数据到所有从节点（使用批处理协议）
     */
    private void sendBatchToSlaves(List<ReplicationBatchProcessor.BatchItem> batchItems) {
        List<String> healthySlaves = replicationState.getHealthySlaveIds().stream().toList();
        if (healthySlaves.isEmpty()) {
            log.debug("没有健康的从节点，跳过批处理发送 - batchSize: {}", batchItems.size());
            return;
        }

        log.debug("发送批处理数据到从节点 - batchSize: {}, slaves: {}, sequences: {}",
                batchItems.size(), healthySlaves.size(), 
                batchItems.stream().mapToLong(ReplicationBatchProcessor.BatchItem::getSequence).toArray());

        // 编码批处理消息
        ByteBuf batchMessage = ReplicationProtocolUtils.encodeBatchBusinessMessage(batchItems);
        
        try {
            // 同步发送到所有从节点
            for (String slaveNodeIdStr : healthySlaves) {
                try {
                    int nodeId = Integer.parseInt(slaveNodeIdStr);
                    
                    // 通过TcpReplicationServer发送批处理数据
                    if (tcpReplicationServer != null) {
                        tcpReplicationServer.sendBatchReplicationRequest(nodeId, batchMessage);
                    }
                    
                    log.debug("成功发送批处理数据到从节点 {} - batchSize: {}", slaveNodeIdStr, batchItems.size());
                    
                } catch (Exception e) {
                    log.error("发送批处理数据失败 - slave: {}, batchSize: {}", slaveNodeIdStr, batchItems.size(), e);
                    replicationState.setSlaveConnected(Integer.parseInt(slaveNodeIdStr), false);
                }
            }
            
            log.debug("批处理数据发送完成 - batchSize: {}", batchItems.size());
            
        } finally {
            // 释放编码后的消息
            batchMessage.release();
        }
    }

    /**
     * 设置TcpReplicationServer引用
     */
    public void setTcpReplicationServer(TcpReplicationServer tcpReplicationServer) {
        this.tcpReplicationServer = tcpReplicationServer;
        log.info("TcpReplicationServer reference set in ReplicationHandler");
    }

    /**
     * 获取性能统计信息
     */
    public PerformanceStats getPerformanceStats() {
        return new PerformanceStats(totalEventsProcessed.get(), batchProcessor.getPerformanceStats());
    }

    /**
     * 性能统计数据结构
     */
    public static class PerformanceStats {
        public final long totalEventsProcessed;
        public final ReplicationBatchProcessor.BatchPerformanceStats batchStats;
        
        public PerformanceStats(long totalEventsProcessed, ReplicationBatchProcessor.BatchPerformanceStats batchStats) {
            this.totalEventsProcessed = totalEventsProcessed;
            this.batchStats = batchStats;
        }
        
        @Override
        public String toString() {
            return String.format("PerformanceStats{events=%d, batchStats=%s}", totalEventsProcessed, batchStats);
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(ReplicationHandler.class.getSimpleName() + "-thread");
        log.info("ReplicationHandler started");
    }

    @Override
    public void onShutdown() {
        log.info("ReplicationHandler shutting down");
        
        // 关闭批处理器
        if (batchProcessor != null) {
            batchProcessor.shutdown();
            log.info("ReplicationBatchProcessor shutdown completed");
        }
        
        // 记录最终统计
        log.info("Final replication stats: {}", getPerformanceStats());
    }
}