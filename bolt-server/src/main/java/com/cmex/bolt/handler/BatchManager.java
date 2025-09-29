package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能批次管理器 - 使用对象池和内存优化
 */
@Slf4j
public class BatchManager {
    
    // 对象池 - 重用ArrayList对象
    private final BlockingQueue<List<NexusWrapper>> batchPool;
    private final int poolSize;
    
    // 性能统计
    private final AtomicLong poolHits = new AtomicLong(0);
    private final AtomicLong poolMisses = new AtomicLong(0);
    private final AtomicLong totalBatchesCreated = new AtomicLong(0);
    
    // 批次元数据
    private final AtomicLong batchSequenceStart = new AtomicLong(-1);
    private final AtomicLong batchSequenceEnd = new AtomicLong(-1);
    private final AtomicLong batchCreationTime = new AtomicLong(0);
    
    public BatchManager(int poolSize) {
        this.poolSize = poolSize;
        this.batchPool = new ArrayBlockingQueue<>(poolSize);
        
        // 预填充对象池
        for (int i = 0; i < poolSize; i++) {
            batchPool.offer(new ArrayList<>());
        }
        
        log.info("BatchManager initialized with pool size: {}", poolSize);
    }
    
    /**
     * 获取批次对象（优先从池中获取）
     */
    public List<NexusWrapper> acquireBatch() {
        List<NexusWrapper> batch = batchPool.poll();
        if (batch != null) {
            batch.clear(); // 清空内容但保留容量
            poolHits.incrementAndGet();
        } else {
            batch = new ArrayList<>();
            poolMisses.incrementAndGet();
        }
        
        totalBatchesCreated.incrementAndGet();
        batchCreationTime.set(System.currentTimeMillis());
        return batch;
    }
    
    /**
     * 释放批次对象回池中
     */
    public void releaseBatch(List<NexusWrapper> batch) {
        if (batch != null && batchPool.offer(batch)) {
            // 成功回收到池中
        } else {
            // 池已满，丢弃对象
            log.debug("Batch pool is full, discarding batch");
        }
    }
    
    /**
     * 更新批次序列范围
     */
    public void updateBatchSequence(long sequence) {
        if (batchSequenceStart.get() == -1) {
            batchSequenceStart.set(sequence);
        }
        batchSequenceEnd.set(sequence);
    }
    
    /**
     * 获取批次序列范围
     */
    public BatchSequenceRange getBatchSequenceRange() {
        return new BatchSequenceRange(
            batchSequenceStart.get(),
            batchSequenceEnd.get(),
            batchCreationTime.get()
        );
    }
    
    /**
     * 重置批次状态
     */
    public void resetBatchState() {
        batchSequenceStart.set(-1);
        batchSequenceEnd.set(-1);
        batchCreationTime.set(0);
    }
    
    /**
     * 获取池统计信息
     */
    public PoolStats getPoolStats() {
        long hits = poolHits.get();
        long misses = poolMisses.get();
        long total = hits + misses;
        double hitRate = total > 0 ? (double) hits / total : 0;
        
        return new PoolStats(
            poolSize,
            batchPool.size(),
            hits,
            misses,
            hitRate,
            totalBatchesCreated.get()
        );
    }
    
    /**
     * 批次序列范围
     */
    public static class BatchSequenceRange {
        public final long start;
        public final long end;
        public final long creationTime;
        
        public BatchSequenceRange(long start, long end, long creationTime) {
            this.start = start;
            this.end = end;
            this.creationTime = creationTime;
        }
        
        public long getAge() {
            return System.currentTimeMillis() - creationTime;
        }
        
        public int getSize() {
            return (int) (end - start + 1);
        }
    }
    
    /**
     * 池统计信息
     */
    public static class PoolStats {
        public final int poolSize;
        public final int availableObjects;
        public final long hits;
        public final long misses;
        public final double hitRate;
        public final long totalCreated;
        
        public PoolStats(int poolSize, int availableObjects, long hits, long misses, 
                        double hitRate, long totalCreated) {
            this.poolSize = poolSize;
            this.availableObjects = availableObjects;
            this.hits = hits;
            this.misses = misses;
            this.hitRate = hitRate;
            this.totalCreated = totalCreated;
        }
        
        @Override
        public String toString() {
            return String.format("PoolStats{poolSize=%d, available=%d, hits=%d, misses=%d, " +
                    "hitRate=%.2f%%, totalCreated=%d}", 
                    poolSize, availableObjects, hits, misses, hitRate * 100, totalCreated);
        }
    }
}

