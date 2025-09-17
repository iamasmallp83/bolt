package com.cmex.bolt.monitor;

import com.cmex.bolt.util.BackpressureManager;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * RingBuffer监控器
 * 负责定期监控多个RingBuffer的状态并输出统计信息
 */
public class RingBufferMonitor {
    private final List<BackpressureManager> managers;
    private final ScheduledExecutorService scheduler;
    private final long reportIntervalMs;
    
    public RingBufferMonitor(long reportIntervalMs) {
        this.managers = new CopyOnWriteArrayList<>();
        this.reportIntervalMs = reportIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "RingBuffer-Monitor");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * 添加需要监控的BackpressureManager
     */
    public void addManager(BackpressureManager manager) {
        managers.add(manager);
    }
    
    /**
     * 移除BackpressureManager
     */
    public void removeManager(BackpressureManager manager) {
        managers.remove(manager);
    }
    
    /**
     * 开始监控
     */
    public void startMonitoring() {
        scheduler.scheduleAtFixedRate(this::reportStats, reportIntervalMs, reportIntervalMs, TimeUnit.MILLISECONDS);
        System.out.println("RingBuffer Monitor started with interval: " + reportIntervalMs + "ms");
    }
    
    /**
     * 停止监控
     */
    public void stopMonitoring() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("RingBuffer Monitor stopped");
    }
    
    /**
     * 立即报告所有统计信息
     */
    public void reportStats() {
        if (managers.isEmpty()) {
            return;
        }
        
        System.out.println("=== RingBuffer Status Report ===");
        for (BackpressureManager manager : managers) {
            BackpressureManager.BackpressureStats stats = manager.getStats();
            
            // 输出基本统计信息
            System.out.println(manager.formatStats());
            
            // 如果有高负载或拒绝，输出警告
            if (stats.isCritical()) {
                System.err.printf("⚠️  CRITICAL: RingBuffer usage is %.2f%% (threshold: %.2f%%)%n", 
                    stats.currentUsageRate() * 100, stats.criticalWatermark() * 100);
            } else if (stats.isHighLoad()) {
                System.out.printf("⚠️  WARNING: RingBuffer usage is %.2f%% (threshold: %.2f%%)%n", 
                    stats.currentUsageRate() * 100, stats.highWatermark() * 100);
            }
            
            // 如果有拒绝率，输出详细信息
            if (stats.rejectedRequests() > 0) {
                System.out.printf("📊 RingBuffer rejection rate: %.2f%% (%d/%d requests)%n",
                    stats.getRejectionRate() * 100, 
                    stats.rejectedRequests(), stats.totalRequests());
            }
        }
        System.out.println("==============================");
    }
    
    /**
     * 获取所有管理器的汇总统计
     */
    public SummaryStats getSummaryStats() {
        if (managers.isEmpty()) {
            return new SummaryStats(0, 0, 0, 0, 0);
        }
        
        long totalRequests = 0;
        long totalRejected = 0;
        double maxUsageRate = 0;
        double avgUsageRate = 0;
        int criticalCount = 0;
        
        for (BackpressureManager manager : managers) {
            BackpressureManager.BackpressureStats stats = manager.getStats();
            totalRequests += stats.totalRequests();
            totalRejected += stats.rejectedRequests();
            maxUsageRate = Math.max(maxUsageRate, stats.currentUsageRate());
            avgUsageRate += stats.currentUsageRate();
            if (stats.isCritical()) {
                criticalCount++;
            }
        }
        
        avgUsageRate /= managers.size();
        
        return new SummaryStats(totalRequests, totalRejected, maxUsageRate, avgUsageRate, criticalCount);
    }
    
    /**
     * 汇总统计信息
     */
    public record SummaryStats(
        long totalRequests,
        long totalRejected,
        double maxUsageRate,
        double avgUsageRate,
        int criticalRingBuffers
    ) {
        public double getRejectionRate() {
            return totalRequests > 0 ? (double)totalRejected / totalRequests : 0.0;
        }
        
        public boolean hasIssues() {
            return criticalRingBuffers > 0 || getRejectionRate() > 0.01; // 1%拒绝率阈值
        }
    }
} 