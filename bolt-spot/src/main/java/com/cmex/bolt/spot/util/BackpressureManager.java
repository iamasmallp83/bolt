package com.cmex.bolt.spot.util;

import com.lmax.disruptor.RingBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * RingBuffer背压管理器
 * 用于监控RingBuffer容量并提供背压控制
 */
public class BackpressureManager {
    private final RingBuffer<?> ringBuffer;
    private final double highWatermark;
    private final double criticalWatermark;
    private final String name;
    
    // 统计信息
    private final LongAdder rejectedRequests = new LongAdder();
    private final LongAdder totalRequests = new LongAdder();
    private final AtomicLong lastReportTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong maxUsageRate = new AtomicLong(0);
    
    public BackpressureManager(String name, RingBuffer<?> ringBuffer) {
        this(name, ringBuffer, 0.6, 0.8);
    }
    
    public BackpressureManager(String name, RingBuffer<?> ringBuffer, double highWatermark, double criticalWatermark) {
        this.name = name;
        this.ringBuffer = ringBuffer;
        this.highWatermark = highWatermark;
        this.criticalWatermark = criticalWatermark;
        
        if (highWatermark >= criticalWatermark) {
            throw new IllegalArgumentException("highWatermark must be less than criticalWatermark");
        }
    }
    
    /**
     * 检查当前容量状态
     * @return 背压结果
     */
    public BackpressureResult checkCapacity() {
        totalRequests.increment();
        
        long capacity = ringBuffer.getBufferSize();
        long remaining = ringBuffer.remainingCapacity();
        double usageRate = (double)(capacity - remaining) / capacity;
        
        // 更新最大使用率
        updateMaxUsageRate(usageRate);
        
        if (usageRate >= criticalWatermark) {
            rejectedRequests.increment();
            return BackpressureResult.CRITICAL_REJECT;
        } else if (usageRate >= highWatermark) {
            return BackpressureResult.HIGH_LOAD;
        } else {
            return BackpressureResult.NORMAL;
        }
    }
    
    /**
     * 简单检查是否可以接受请求
     * @return true如果可以接受，false如果应该拒绝
     */
    public boolean canAcceptRequest() {
        return checkCapacity() != BackpressureResult.CRITICAL_REJECT;
    }
    
    /**
     * 获取当前使用率
     * @return 0.0-1.0之间的使用率
     */
    public double getCurrentUsageRate() {
        long capacity = ringBuffer.getBufferSize();
        long remaining = ringBuffer.remainingCapacity();
        return (double)(capacity - remaining) / capacity;
    }
    
    /**
     * 更新最大使用率（线程安全）
     */
    private void updateMaxUsageRate(double usageRate) {
        long currentMax = maxUsageRate.get();
        long newMax = Double.doubleToLongBits(usageRate);
        while (usageRate > Double.longBitsToDouble(currentMax)) {
            if (maxUsageRate.compareAndSet(currentMax, newMax)) {
                break;
            }
            currentMax = maxUsageRate.get();
        }
    }
    
    /**
     * 获取统计信息
     * @return 背压统计信息
     */
    public BackpressureStats getStats() {
        long capacity = ringBuffer.getBufferSize();
        long remaining = ringBuffer.remainingCapacity();
        double currentUsageRate = (double)(capacity - remaining) / capacity;
        double maxUsage = Double.longBitsToDouble(maxUsageRate.get());
        
        return new BackpressureStats(
            name,
            currentUsageRate,
            maxUsage,
            rejectedRequests.sum(),
            totalRequests.sum(),
            capacity,
            remaining,
            highWatermark,
            criticalWatermark
        );
    }
    
    /**
     * 重置统计信息
     */
    public void resetStats() {
        rejectedRequests.reset();
        totalRequests.reset();
        maxUsageRate.set(0);
        lastReportTime.set(System.currentTimeMillis());
    }
    
    /**
     * 检查是否需要打印警告日志
     * @param reportIntervalMs 报告间隔（毫秒）
     * @return true如果需要报告
     */
    public boolean shouldReport(long reportIntervalMs) {
        long now = System.currentTimeMillis();
        long lastReport = lastReportTime.get();
        if (now - lastReport >= reportIntervalMs) {
            return lastReportTime.compareAndSet(lastReport, now);
        }
        return false;
    }
    
    /**
     * 格式化统计信息用于日志输出
     */
    public String formatStats() {
        BackpressureStats stats = getStats();
        return String.format("[%s] Usage: %.2f%% (Max: %.2f%%), Rejected: %d/%d (%.2f%%), Remaining: %d/%d",
            stats.name(),
            stats.currentUsageRate() * 100,
            stats.maxUsageRate() * 100,
            stats.rejectedRequests(),
            stats.totalRequests(),
            stats.totalRequests() > 0 ? (double)stats.rejectedRequests() / stats.totalRequests() * 100 : 0.0,
            stats.remaining(),
            stats.capacity()
        );
    }
    
    /**
     * 背压检查结果
     */
    public enum BackpressureResult {
        NORMAL,           // 正常，可以接受请求
        HIGH_LOAD,        // 高负载，可以接受但需要警告
        CRITICAL_REJECT   // 临界状态，拒绝请求
    }
    
    /**
     * 背压统计信息
     */
    public record BackpressureStats(
        String name,
        double currentUsageRate,
        double maxUsageRate,
        long rejectedRequests,
        long totalRequests,
        long capacity,
        long remaining,
        double highWatermark,
        double criticalWatermark
    ) {
        public double getRejectionRate() {
            return totalRequests > 0 ? (double)rejectedRequests / totalRequests : 0.0;
        }
        
        public boolean isHighLoad() {
            return currentUsageRate >= highWatermark;
        }
        
        public boolean isCritical() {
            return currentUsageRate >= criticalWatermark;
        }
    }
} 