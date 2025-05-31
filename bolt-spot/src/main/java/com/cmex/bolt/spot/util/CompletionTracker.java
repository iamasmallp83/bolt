package com.cmex.bolt.spot.util;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 处理完成跟踪器
 * 用于在有背压控制的情况下判断所有请求是否处理完成
 */
public class CompletionTracker {
    private final SpotServiceImpl service;
    private final AtomicLong sentRequests = new AtomicLong(0);
    private final AtomicLong processedRequests = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong rejectedRequests = new AtomicLong(0);
    
    // 处理效率统计
    private final AtomicLong startTime = new AtomicLong(0);
    private final AtomicLong endTime = new AtomicLong(0);
    private final AtomicLong firstRequestTime = new AtomicLong(0);
    private final AtomicLong lastResponseTime = new AtomicLong(0);
    private final AtomicLong requestStartTime = new AtomicLong(0); // 请求开始发送时间
    
    // 配置参数
    private final long maxWaitTimeMs;
    private final long checkIntervalMs;
    private final int maxStableChecks;
    
    public CompletionTracker(SpotServiceImpl service) {
        this(service, 60000, 500, 10); // 默认最多等待60秒，每500ms检查一次，连续10次无变化认为完成
    }
    
    public CompletionTracker(SpotServiceImpl service, long maxWaitTimeMs, long checkIntervalMs, int maxStableChecks) {
        this.service = service;
        this.maxWaitTimeMs = maxWaitTimeMs;
        this.checkIntervalMs = checkIntervalMs;
        this.maxStableChecks = maxStableChecks;
    }
    
    /**
     * 开始统计（在发送第一个请求前调用）
     */
    public void start() {
        long currentTime = System.nanoTime();
        startTime.set(currentTime);
        requestStartTime.set(currentTime);
        firstRequestTime.compareAndSet(0, currentTime);
    }
    
    /**
     * 记录发送的请求
     */
    public void recordSentRequest() {
        sentRequests.incrementAndGet();
        // 记录第一个请求的时间
        firstRequestTime.compareAndSet(0, System.nanoTime());
    }
    
    /**
     * 记录成功处理的请求
     */
    public void recordSuccessfulRequest() {
        long responseTime = System.nanoTime();
        successfulRequests.incrementAndGet();
        processedRequests.incrementAndGet();
        updateLastResponseTime(responseTime);
    }
    
    /**
     * 记录被拒绝的请求
     */
    public void recordRejectedRequest() {
        long responseTime = System.nanoTime();
        rejectedRequests.incrementAndGet();
        processedRequests.incrementAndGet();
        updateLastResponseTime(responseTime);
    }
    
    /**
     * 更新最后响应时间
     */
    private void updateLastResponseTime(long responseTime) {
        lastResponseTime.set(responseTime);
    }
    
    /**
     * 结束统计
     */
    public void finish() {
        endTime.set(System.nanoTime());
    }
    
    /**
     * 等待所有请求处理完成
     * @return 完成结果
     */
    public CompletionResult waitForCompletion() throws InterruptedException {
        System.out.println("=== Waiting for processing completion ===");
        
        long startWaitTime = System.currentTimeMillis();
        long lastProcessed = 0;
        int stableCount = 0;
        
        while (System.currentTimeMillis() - startWaitTime < maxWaitTimeMs) {
            long currentProcessed = processedRequests.get();
            long currentSent = sentRequests.get();
            
            // 打印进度
            printProgress(currentProcessed, currentSent);
            
            // 检查是否所有请求都有响应
            if (currentProcessed >= currentSent && currentSent > 0) {
                System.out.println("✅ All requests processed successfully!");
                finish();
                return new CompletionResult(true, false, getCurrentStats());
            }
            
            // 检查进度是否停滞
            if (currentProcessed == lastProcessed) {
                stableCount++;
                if (stableCount >= maxStableChecks) {
                    System.out.println("⚠️ Progress stalled, analyzing system status...");
                    
                    SystemHealthStatus health = analyzeSystemHealth();
                    if (health.isDeadlocked()) {
                        System.out.println("💀 System deadlock detected!");
                        finish();
                        return new CompletionResult(false, true, getCurrentStats());
                    } else if (health.isStabilized()) {
                        System.out.println("✅ System has stabilized with partial completion");
                        finish();
                        return new CompletionResult(true, false, getCurrentStats());
                    }
                    
                    // 继续等待
                    stableCount = 0;
                }
            } else {
                stableCount = 0;
                lastProcessed = currentProcessed;
            }
            
            TimeUnit.MILLISECONDS.sleep(checkIntervalMs);
        }
        
        System.out.println("⏰ Timeout reached");
        finish();
        return new CompletionResult(false, false, getCurrentStats());
    }
    
    /**
     * 打印当前进度（包含效率统计）
     */
    private void printProgress(long processed, long sent) {
        double percentage = sent > 0 ? (double) processed / sent * 100 : 0;
        
        // 计算当前处理速率
        long currentTime = System.nanoTime();
        long elapsedMs = (currentTime - firstRequestTime.get()) / 1_000_000;
        double throughput = processed > 0 && elapsedMs > 0 ? (double) processed / elapsedMs * 1000 : 0;
        
        System.out.printf("Progress: %d/%d (%.2f%%) | Success: %d, Rejected: %d | Throughput: %.2f req/s%n",
                processed, sent, percentage, successfulRequests.get(), rejectedRequests.get(), throughput);
    }
    
    /**
     * 分析系统健康状态
     */
    private SystemHealthStatus analyzeSystemHealth() {
        try {
            var summary = service.getMonitorSummary();
            
            System.out.println("--- System Health Analysis ---");
            System.out.printf("Ring buffer usage - Max: %.2f%%, Avg: %.2f%%\n",
                    summary.maxUsageRate() * 100, summary.avgUsageRate() * 100);
            System.out.printf("System rejection rate: %.2f%% (%d/%d)\n",
                    summary.getRejectionRate() * 100, summary.totalRejected(), summary.totalRequests());
            System.out.printf("Critical ring buffers: %d\n", summary.criticalRingBuffers());
            
            // 判断是否死锁：所有RingBuffer都满且没有处理进度
            boolean allBuffersFull = summary.maxUsageRate() > 0.95;
            boolean highRejectionRate = summary.getRejectionRate() > 0.5;
            boolean hasCriticalBuffers = summary.criticalRingBuffers() > 0;
            
            if (allBuffersFull && hasCriticalBuffers) {
                return new SystemHealthStatus(true, false); // 死锁
            }
            
            // 判断是否稳定：拒绝率高但系统仍在工作
            if (highRejectionRate && !hasCriticalBuffers) {
                return new SystemHealthStatus(false, true); // 稳定
            }
            
            return new SystemHealthStatus(false, false); // 继续等待
            
        } catch (Exception e) {
            System.err.println("Error analyzing system health: " + e.getMessage());
            return new SystemHealthStatus(false, false);
        }
    }
    
    /**
     * 获取当前统计信息
     */
    public CompletionStats getCurrentStats() {
        long currentEndTime = endTime.get() > 0 ? endTime.get() : System.nanoTime();
        long totalElapsedNs = currentEndTime - startTime.get();
        long processingElapsedNs = lastResponseTime.get() > 0 ? lastResponseTime.get() - firstRequestTime.get() : 0;
        
        return new CompletionStats(
                sentRequests.get(),
                processedRequests.get(), 
                successfulRequests.get(),
                rejectedRequests.get(),
                totalElapsedNs,
                processingElapsedNs,
                0 // 累计处理时间
        );
    }
    
    /**
     * 重置所有计数器
     */
    public void reset() {
        sentRequests.set(0);
        processedRequests.set(0);
        successfulRequests.set(0);
        rejectedRequests.set(0);
        startTime.set(0);
        endTime.set(0);
        firstRequestTime.set(0);
        lastResponseTime.set(0);
        requestStartTime.set(0);
    }
    
    /**
     * 系统健康状态
     */
    private record SystemHealthStatus(boolean isDeadlocked, boolean isStabilized) {}
    
    /**
     * 完成统计信息（增强版，包含处理效率统计）
     */
    public record CompletionStats(
            long sentRequests,
            long processedRequests,
            long successfulRequests,
            long rejectedRequests,
            long totalElapsedNs,      // 总耗时（纳秒）
            long processingElapsedNs, // 处理耗时（从第一个请求到最后一个响应）
            long unused // 保留兼容性，不再使用
    ) {
        public double getSuccessRate() {
            return sentRequests > 0 ? (double) successfulRequests / sentRequests : 0.0;
        }
        
        public double getRejectionRate() {
            return sentRequests > 0 ? (double) rejectedRequests / sentRequests : 0.0;
        }
        
        public double getCompletionRate() {
            return sentRequests > 0 ? (double) processedRequests / sentRequests : 0.0;
        }
        
        /**
         * 获取总耗时（毫秒）
         */
        public double getTotalElapsedMs() {
            return totalElapsedNs / 1_000_000.0;
        }
        
        /**
         * 获取处理耗时（毫秒）
         */
        public double getProcessingElapsedMs() {
            return processingElapsedNs / 1_000_000.0;
        }
        
        /**
         * 获取平均处理时间（毫秒）
         */
        public double getAverageProcessingTimeMs() {
            return processedRequests > 0 ? getProcessingElapsedMs() / processedRequests : 0.0;
        }
        
        /**
         * 获取吞吐量（请求/秒）
         */
        public double getThroughput() {
            double elapsedSec = processingElapsedNs / 1_000_000_000.0;
            return elapsedSec > 0 ? processedRequests / elapsedSec : 0.0;
        }
        
        /**
         * 获取成功订单的吞吐量（订单/秒）
         */
        public double getSuccessfulThroughput() {
            double elapsedSec = processingElapsedNs / 1_000_000_000.0;
            return elapsedSec > 0 ? successfulRequests / elapsedSec : 0.0;
        }
    }
    
    /**
     * 完成结果（增强版）
     */
    public record CompletionResult(
            boolean completed,
            boolean deadlocked,
            CompletionStats stats
    ) {
        public void printSummary() {
            System.out.println("\n=== Completion Summary ===");
            System.out.printf("Status: %s\n", completed ? "✅ COMPLETED" : (deadlocked ? "💀 DEADLOCKED" : "⏰ TIMEOUT"));
            System.out.printf("Sent: %d, Processed: %d (%.2f%%)\n", 
                    stats.sentRequests, stats.processedRequests, stats.getCompletionRate() * 100);
            System.out.printf("Success: %d (%.2f%%), Rejected: %d (%.2f%%)\n",
                    stats.successfulRequests, stats.getSuccessRate() * 100,
                    stats.rejectedRequests, stats.getRejectionRate() * 100);
        }
        
        /**
         * 打印详细的处理效率统计
         */
        public void printPerformanceStats() {
            System.out.println("\n=== Performance Statistics ===");
            System.out.printf("📊 Total Orders Processed: %d\n", stats.processedRequests);
            System.out.printf("📊 Successful Orders: %d\n", stats.successfulRequests);
            System.out.printf("⏱️ Total Elapsed Time: %.2f ms\n", stats.getTotalElapsedMs());
            System.out.printf("⏱️ Processing Time: %.2f ms\n", stats.getProcessingElapsedMs());
            System.out.printf("⏱️ Average Processing Time per Order: %.6f ms\n", stats.getAverageProcessingTimeMs());
            System.out.printf("🚀 Overall Throughput: %.2f requests/sec\n", stats.getThroughput());
            System.out.printf("🚀 Successful Order Throughput: %.2f orders/sec\n", stats.getSuccessfulThroughput());
            
            // 性能等级评估
            double throughput = stats.getThroughput();
            if (throughput > 50000) {
                System.out.println("🏆 EXCELLENT performance (>50K req/s)");
            } else if (throughput > 20000) {
                System.out.println("✅ GOOD performance (>20K req/s)");
            } else if (throughput > 10000) {
                System.out.println("⚠️ ACCEPTABLE performance (>10K req/s)");
            } else {
                System.out.println("❌ POOR performance (<10K req/s)");
            }
            
            // 延迟评估
            double avgLatency = stats.getAverageProcessingTimeMs();
            if (avgLatency < 1.0) {
                System.out.println("🏆 EXCELLENT latency (<1ms avg)");
            } else if (avgLatency < 5.0) {
                System.out.println("✅ GOOD latency (<5ms avg)");
            } else if (avgLatency < 10.0) {
                System.out.println("⚠️ ACCEPTABLE latency (<10ms avg)");
            } else {
                System.out.println("❌ HIGH latency (>10ms avg)");
            }
        }
    }
} 