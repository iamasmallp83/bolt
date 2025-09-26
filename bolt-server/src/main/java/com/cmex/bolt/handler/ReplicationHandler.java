package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 实现批量确认机制，确保强一致性
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    @Getter
    private final Sequence sequence = new Sequence();
    
    private final BoltConfig config;
    private final ReplicationState replicationState;
    private final List<NexusWrapper> currentBatch;
    private final AtomicLong batchSequenceStart = new AtomicLong(-1);
    private final AtomicLong batchSequenceEnd = new AtomicLong(-1);
    
    // 复制客户端管理器（稍后实现）
    private ReplicationClientManager clientManager;
    
    public ReplicationHandler(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;
        this.currentBatch = new ArrayList<>(config.batchSize());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查
        if (wrapper.getBuffer() == null || wrapper.getBuffer().readableBytes() == 0 || wrapper.getId() == -1) {
            this.sequence.set(sequence);
            return;
        }
        
        // 添加到当前批次
        synchronized (currentBatch) {
            currentBatch.add(wrapper);
            
            // 设置批次序列范围
            if (batchSequenceStart.get() == -1) {
                batchSequenceStart.set(sequence);
            }
            batchSequenceEnd.set(sequence);
            
            // 检查是否需要发送批次
            boolean shouldSendBatch = currentBatch.size() >= config.batchSize() || endOfBatch;
            
            if (shouldSendBatch) {
                sendBatchToSlaves();
            }
        }
        
        this.sequence.set(sequence);
    }
    
    /**
     * 发送批次到从节点
     */
    private void sendBatchToSlaves() {
        List<NexusWrapper> batchToSend;
        long sequenceStart, sequenceEnd;
        
        synchronized (currentBatch) {
            if (currentBatch.isEmpty()) {
                return;
            }
            
            batchToSend = new ArrayList<>(currentBatch);
            sequenceStart = batchSequenceStart.get();
            sequenceEnd = batchSequenceEnd.get();
            
            // 清空当前批次
            currentBatch.clear();
            batchSequenceStart.set(-1);
            batchSequenceEnd.set(-1);
        }
        
        // 创建批次跟踪器
        ReplicationState.BatchAckTracker tracker = replicationState.createBatchTracker(
                config.batchTimeoutMs(), sequenceStart, sequenceEnd);
        
        log.debug("Sending batch {} with {} events, sequences {}-{}", 
                tracker.getBatchId(), batchToSend.size(), sequenceStart, sequenceEnd);
        
        // 发送到所有从节点
        if (clientManager != null) {
            clientManager.sendBatchToSlaves(tracker.getBatchId(), batchToSend, sequenceStart, sequenceEnd);
        }
        
        // 等待确认（非阻塞方式）
        waitForBatchAcknowledgment(tracker);
    }
    
    /**
     * 等待批次确认
     */
    private void waitForBatchAcknowledgment(ReplicationState.BatchAckTracker tracker) {
        // 使用异步方式等待确认，避免阻塞主流程
        new Thread(() -> {
            long startTime = System.currentTimeMillis();
            
            while (!tracker.isAllAcknowledged() && !tracker.isTimeout()) {
                try {
                    Thread.sleep(10); // 短暂休眠，避免CPU占用过高
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            long waitTime = System.currentTimeMillis() - startTime;
            
            if (tracker.isAllAcknowledged()) {
                log.debug("Batch {} acknowledged by all slaves in {}ms", 
                        tracker.getBatchId(), waitTime);
            } else if (tracker.isTimeout()) {
                log.warn("Batch {} timeout after {}ms, {} slaves pending: {}", 
                        tracker.getBatchId(), waitTime, tracker.getPendingCount(), 
                        tracker.getTimeoutSlaves());
                
                // 处理超时的从节点
                handleSlaveTimeout(tracker);
            }
        }, "batch-ack-waiter-" + tracker.getBatchId()).start();
    }
    
    /**
     * 处理从节点超时
     */
    private void handleSlaveTimeout(ReplicationState.BatchAckTracker tracker) {
        for (String slaveNodeId : tracker.getTimeoutSlaves()) {
            log.warn("Slave node {} timeout for batch {}", slaveNodeId, tracker.getBatchId());
            
            // 标记从节点为不健康状态
            replicationState.setSlaveConnected(slaveNodeId, false);
            
            // 可以在这里实现重连逻辑或故障转移
        }
    }
    
    /**
     * 处理从节点的确认
     */
    public void handleBatchAcknowledgment(long batchId, String slaveNodeId) {
        boolean acknowledged = replicationState.acknowledgeBatch(batchId, slaveNodeId);
        if (acknowledged) {
            log.debug("Received ack for batch {} from slave {}", batchId, slaveNodeId);
        } else {
            log.warn("Received invalid ack for batch {} from slave {}", batchId, slaveNodeId);
        }
    }
    
    /**
     * 设置复制客户端管理器
     */
    public void setClientManager(ReplicationClientManager clientManager) {
        this.clientManager = clientManager;
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
        
        // 发送剩余批次
        synchronized (currentBatch) {
            if (!currentBatch.isEmpty()) {
                sendBatchToSlaves();
            }
        }
    }
}