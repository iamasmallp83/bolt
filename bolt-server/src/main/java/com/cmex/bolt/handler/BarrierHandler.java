package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 屏障处理器 - 负责协调从节点确认机制
 * 确保只有当前slave返回确认消息之后，才开始处理sequencerDispatchers
 */
@Slf4j
public class BarrierHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private static final Logger log = LoggerFactory.getLogger(BarrierHandler.class);

    @Getter
    private final Sequence sequence = new Sequence();
    
    private final BoltConfig config;
    private final ReplicationState replicationState;
    private final Map<Long, BatchBarrier> batchBarriers = new ConcurrentHashMap<>();
    private final AtomicLong nextBarrierId = new AtomicLong(1);
    private final ReplicationHandler replicationHandler;
    
    public BarrierHandler(BoltConfig config, ReplicationState replicationState, ReplicationHandler replicationHandler) {
        this.config = config;
        this.replicationState = replicationState;
        this.replicationHandler = replicationHandler;
        
        log.info("BarrierHandler initialized - batchSize: {}, barrierTimeoutMs: {}", 
                config.batchSize(), config.batchTimeout());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查
        if (wrapper.getBuffer() == null || wrapper.getBuffer().readableBytes() == 0 || wrapper.getId() == -1) {
            this.sequence.set(sequence);
            return;
        }
        
        // 检查是否需要等待从节点确认
        if (hasActiveSlaves()) {
            waitForSlaveConfirmation(sequence, endOfBatch);
        }
        
        this.sequence.set(sequence);
    }
    
    /**
     * 等待从节点确认
     */
    private void waitForSlaveConfirmation(long sequence, boolean endOfBatch) {
        // 获取当前批次ID（从ReplicationHandler获取）
        Long currentBatchId = getCurrentBatchId();
        if (currentBatchId == null) {
            log.debug("No active batch found for sequence {}, skipping barrier", sequence);
            return;
        }
        
        BatchBarrier barrier = batchBarriers.get(currentBatchId);
        if (barrier == null) {
            log.debug("No barrier found for batch {}, creating new barrier", currentBatchId);
            barrier = createBarrier(currentBatchId, sequence);
        }
        
        // 如果是批次结束，等待所有从节点确认
        if (endOfBatch) {
            log.info("Waiting for slave confirmation for batch {} (sequence {})", currentBatchId, sequence);
            try {
                boolean confirmed = barrier.waitForConfirmation(config.batchTimeout(), TimeUnit.MILLISECONDS);
                if (confirmed) {
                    log.info("All slaves confirmed batch {} in {}ms", currentBatchId, barrier.getWaitTime());
                } else {
                    log.warn("Batch {} confirmation timeout after {}ms, {} slaves pending", 
                            currentBatchId, config.batchTimeout(), barrier.getPendingCount());
                    handleTimeoutSlaves(barrier);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Barrier wait interrupted for batch {}", currentBatchId);
            } finally {
                // 清理屏障
                batchBarriers.remove(currentBatchId);
            }
        }
    }
    
    /**
     * 创建新的批次屏障
     */
    private BatchBarrier createBarrier(long batchId, long sequence) {
        List<String> healthySlaves = replicationState.getHealthySlaveIds().stream().toList();
        BatchBarrier barrier = new BatchBarrier(batchId, healthySlaves, sequence);
        batchBarriers.put(batchId, barrier);
        
        log.debug("Created barrier for batch {} with {} slaves", batchId, healthySlaves.size());
        return barrier;
    }
    
    /**
     * 处理超时的从节点
     */
    private void handleTimeoutSlaves(BatchBarrier barrier) {
        for (String slaveNodeId : barrier.getTimeoutSlaves()) {
            log.warn("Slave node {} timeout for batch {}", slaveNodeId, barrier.getBatchId());
            replicationState.setSlaveConnected(slaveNodeId, false);
        }
    }
    
    /**
     * 检查是否有活跃的从节点
     */
    private boolean hasActiveSlaves() {
        return !replicationState.getHealthySlaveIds().isEmpty();
    }
    
    /**
     * 获取当前批次ID（从ReplicationHandler获取）
     */
    private Long getCurrentBatchId() {
        if (replicationHandler != null) {
            long batchId = replicationHandler.getCurrentBatchId();
            return batchId > 0 ? batchId : null;
        }
        return null;
    }
    
    /**
     * 处理从节点的确认
     */
    public void handleSlaveConfirmation(long batchId, String slaveNodeId, long sequence) {
        BatchBarrier barrier = batchBarriers.get(batchId);
        if (barrier != null) {
            boolean confirmed = barrier.confirm(slaveNodeId, sequence);
            if (confirmed) {
                log.debug("Slave {} confirmed batch {} for sequence {}", slaveNodeId, batchId, sequence);
            } else {
                log.warn("Invalid confirmation from slave {} for batch {}", slaveNodeId, batchId);
            }
        } else {
            log.warn("Received confirmation for unknown batch {} from slave {}", batchId, slaveNodeId);
        }
    }
    
    /**
     * 批次屏障 - 管理单个批次的确认状态
     */
    private static class BatchBarrier {
        private final long batchId;
        private final CountDownLatch confirmationLatch;
        private final Map<String, Long> slaveConfirmations = new ConcurrentHashMap<>();
        private final long startTime;
        private final long sequence;
        
        public BatchBarrier(long batchId, List<String> slaveNodeIds, long sequence) {
            this.batchId = batchId;
            this.confirmationLatch = new CountDownLatch(slaveNodeIds.size());
            this.startTime = System.currentTimeMillis();
            this.sequence = sequence;
            
            // 初始化从节点确认状态
            for (String slaveNodeId : slaveNodeIds) {
                slaveConfirmations.put(slaveNodeId, -1L);
            }
        }
        
        public boolean confirm(String slaveNodeId, long confirmedSequence) {
            if (slaveConfirmations.containsKey(slaveNodeId)) {
                slaveConfirmations.put(slaveNodeId, confirmedSequence);
                confirmationLatch.countDown();
                return true;
            }
            return false;
        }
        
        public boolean waitForConfirmation(long timeout, TimeUnit unit) throws InterruptedException {
            return confirmationLatch.await(timeout, unit);
        }
        
        public long getWaitTime() {
            return System.currentTimeMillis() - startTime;
        }
        
        public int getPendingCount() {
            return (int) confirmationLatch.getCount();
        }
        
        public List<String> getTimeoutSlaves() {
            return slaveConfirmations.entrySet().stream()
                    .filter(entry -> entry.getValue() == -1L)
                    .map(Map.Entry::getKey)
                    .toList();
        }
        
        public long getBatchId() {
            return batchId;
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(BarrierHandler.class.getSimpleName() + "-thread");
        log.info("BarrierHandler started");
    }

    @Override
    public void onShutdown() {
        log.info("BarrierHandler shutting down");
        
        // 清理所有未完成的屏障
        for (BatchBarrier barrier : batchBarriers.values()) {
            log.warn("Cleaning up barrier for batch {} with {} pending confirmations", 
                    barrier.getBatchId(), barrier.getPendingCount());
        }
        batchBarriers.clear();
    }
}
