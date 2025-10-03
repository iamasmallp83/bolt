package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 复制批处理器 - 负责按顺序聚合和发送复制数据
 * 解决异步发送的顺序问题和批处理优化
 */
@Slf4j
public class ReplicationBatchProcessor {
    
    private final int batchSize;
    private final long batchTimeoutMs;
    private final BatchSender batchSender;
    
    // 注意：不再需要pendingBatches，因为Disruptor已保证顺序
    
    // 当前批处理的起始sequence
    private volatile long batchStartSequence = -1;
    
    // 批处理定时器
    private final ScheduledExecutorService scheduler;
    
    // 使用AtomicReference避免锁竞争
    private final AtomicReference<List<BatchItem>> currentBatch = new AtomicReference<>(new ArrayList<>());
    
    // 批次开始时间（用于1ms超时检查）
    private volatile long batchStartTime = 0;
    
    // 是否已经有定时任务在运行
    private final AtomicReference<ScheduledFuture<?>> pendingTimeoutTask = new AtomicReference<>();
    
    // 性能统计
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalEventsBatched = new AtomicLong(0);

    public ReplicationBatchProcessor(int batchSize, long batchTimeoutMs, BatchSender batchSender) {
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.batchSender = batchSender;
        
        // 创建定时器（单线程）
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "replication-batch-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // 启动定时批处理任务
        startBatchTimer();
        
        log.info("ReplicationBatchProcessor initialized - batchSize: {}, batchTimeout: {}ms", 
                batchSize, batchTimeoutMs);
    }

    /**
     * 添加事件到批处理器（智能批处理策略）
     * 策略：聚齐100条立即发送，否则最多等待1ms
     */
    public void addEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
        // 创建批处理项
        BatchItem item = new BatchItem(wrapper, sequence);
        
        // 使用CAS操作添加到当前批次
        List<BatchItem> current = currentBatch.get();
        current.add(item);
        
        log.debug("Added event to batch - sequence: {}, batchSize: {}", sequence, current.size());
        
        // 策略1：如果达到批处理大小，立即发送
        if (current.size() >= batchSize) {
            log.debug("Batch size reached ({}), sending immediately", batchSize);
            sendCurrentBatch(endOfBatch);
            return;
        }
        
        // 策略2：如果是批次结束，立即发送
        if (endOfBatch) {
            log.debug("End of batch reached, sending immediately");
            sendCurrentBatch(endOfBatch);
            return;
        }
        
        // 策略3：如果这是第一个事件，启动1ms超时定时器
        if (current.size() == 1) {
            batchStartTime = System.currentTimeMillis();
            scheduleTimeoutTask();
        }
    }

    /**
     * 调度1ms超时任务
     */
    private void scheduleTimeoutTask() {
        // 取消之前的超时任务（如果存在）
        ScheduledFuture<?> existingTask = pendingTimeoutTask.getAndSet(null);
        if (existingTask != null) {
            existingTask.cancel(false);
        }
        
        // 创建新的1ms超时任务
        ScheduledFuture<?> timeoutTask = scheduler.schedule(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                long elapsed = currentTime - batchStartTime;
                
                // 检查是否已经超过1ms
                if (elapsed >= 1) {
                    log.debug("1ms timeout reached, sending batch");
                    sendCurrentBatch(false);
                } else {
                    // 如果还没到1ms，重新调度
                    long remainingTime = 1 - elapsed;
                    scheduler.schedule(() -> {
                        log.debug("1ms timeout reached (rescheduled), sending batch");
                        sendCurrentBatch(false);
                    }, remainingTime, TimeUnit.MILLISECONDS);
                }
            } catch (Exception e) {
                log.error("Error in timeout task", e);
            }
        }, 1, TimeUnit.MILLISECONDS);
        
        // 保存任务引用
        pendingTimeoutTask.set(timeoutTask);
    }

    /**
     * 发送当前批次（单线程同步发送）
     */
    private void sendCurrentBatch(boolean endOfBatch) {
        // 取消待处理的超时任务
        ScheduledFuture<?> timeoutTask = pendingTimeoutTask.getAndSet(null);
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
        
        // 原子性地获取并重置当前批次
        List<BatchItem> batchToSend = currentBatch.getAndSet(new ArrayList<>());
        
        if (!batchToSend.isEmpty()) {
            // 记录批次起始sequence
            if (batchStartSequence == -1) {
                batchStartSequence = batchToSend.get(0).getSequence();
            }
            
            log.debug("Sending batch - size: {}, startSequence: {}", 
                    batchToSend.size(), batchStartSequence);
            
            // 同步发送批处理数据
            try {
                batchSender.sendBatch(batchToSend);
                totalBatchesSent.incrementAndGet();
                totalEventsBatched.addAndGet(batchToSend.size());
            } catch (Exception e) {
                log.error("Failed to send batch - size: {}", batchToSend.size(), e);
            }
            
            // 重置批次起始sequence和时间
            batchStartSequence = -1;
            batchStartTime = 0;
        }
    }


    /**
     * 启动定时批处理任务（无锁设计）
     */
    private void startBatchTimer() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                // 检查当前批次是否有超时的项目
                List<BatchItem> current = currentBatch.get();
                if (!current.isEmpty()) {
                    long currentTime = System.currentTimeMillis();
                    BatchItem firstItem = current.get(0);
                    
                    // 如果第一个项目超时，发送整个批次
                    if (currentTime - firstItem.getTimestamp() > batchTimeoutMs) {
                        log.debug("Sending timeout batch - size: {}", current.size());
                        sendCurrentBatch(false);
                    }
                }
                
            } catch (Exception e) {
                log.error("Error in batch timer", e);
            }
        }, batchTimeoutMs, batchTimeoutMs / 2, TimeUnit.MILLISECONDS);
    }


    /**
     * 获取性能统计
     */
    public BatchPerformanceStats getPerformanceStats() {
        return new BatchPerformanceStats(
            totalBatchesSent.get(),
            totalEventsBatched.get(),
            currentBatch.get().size(),
            batchStartSequence
        );
    }

    /**
     * 关闭批处理器（单线程设计）
     */
    public void shutdown() {
        log.info("Shutting down ReplicationBatchProcessor");
        
        // 取消所有待处理的超时任务
        ScheduledFuture<?> timeoutTask = pendingTimeoutTask.getAndSet(null);
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
        
        // 发送剩余的所有批处理
        List<BatchItem> remainingItems = currentBatch.getAndSet(new ArrayList<>());
        if (!remainingItems.isEmpty()) {
            log.info("Sending remaining {} items before shutdown", remainingItems.size());
            sendCurrentBatch(true);
        }
        
        // 关闭定时器
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        log.info("ReplicationBatchProcessor shutdown completed - final stats: {}", getPerformanceStats());
    }

    /**
     * 批处理项 - 包含单个事件的数据
     */
    public static class BatchItem {
        private final NexusWrapper wrapper;
        private final long sequence;
        private final long timestamp;
        private final byte[] bufferCopy;

        public BatchItem(NexusWrapper wrapper, long sequence) {
            this.wrapper = wrapper;
            this.sequence = sequence;
            this.timestamp = System.currentTimeMillis();
            
            // 创建buffer的拷贝以避免并发问题
            var originalBuffer = wrapper.getBuffer();
            int readableBytes = originalBuffer.readableBytes();
            this.bufferCopy = new byte[readableBytes];
            
            int originalReaderIndex = originalBuffer.readerIndex();
            originalBuffer.readBytes(bufferCopy);
            originalBuffer.readerIndex(originalReaderIndex);
        }

        public NexusWrapper getWrapper() { return wrapper; }
        public long getSequence() { return sequence; }
        public long getTimestamp() { return timestamp; }
        public byte[] getBufferCopy() { return bufferCopy; }
    }

    /**
     * 批处理发送接口
     */
    public interface BatchSender {
        void sendBatch(List<BatchItem> batchItems);
    }

    /**
     * 批处理性能统计
     */
    public static class BatchPerformanceStats {
        public final long totalBatchesSent;
        public final long totalEventsBatched;
        public final int currentBatchSize;
        public final long batchStartSequence;
        
        public BatchPerformanceStats(long totalBatchesSent, long totalEventsBatched, 
                                   int currentBatchSize, long batchStartSequence) {
            this.totalBatchesSent = totalBatchesSent;
            this.totalEventsBatched = totalEventsBatched;
            this.currentBatchSize = currentBatchSize;
            this.batchStartSequence = batchStartSequence;
        }
        
        @Override
        public String toString() {
            return String.format("BatchPerformanceStats{batches=%d, events=%d, currentBatchSize=%d, batchStartSeq=%d}", 
                    totalBatchesSent, totalEventsBatched, currentBatchSize, batchStartSequence);
        }
    }
}
