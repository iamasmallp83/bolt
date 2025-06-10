package com.cmex.bolt.server.util;

import com.cmex.bolt.server.grpc.EnvoyServer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 系统完成状态检测器
 * 用于判断复杂异步系统中所有订单是否真正处理完成
 * 
 * 核心原理：
 * 1. 多维度状态监控：发送计数、处理计数、背压状态、RingBuffer状态
 * 2. 稳定性检测：连续多次检查无变化才认为完成
 * 3. 超时保护：防止无限等待
 * 4. 分层状态跟踪：Account、Match、Response三层状态
 */
public class SystemCompletionDetector {
    
    private final EnvoyServer service;
    private final List<BackpressureManager> backpressureManagers;
    
    // 核心计数器
    private final AtomicLong sentRequests = new AtomicLong(0);
    private final AtomicLong accountProcessed = new AtomicLong(0);
    private final AtomicLong matchProcessed = new AtomicLong(0);
    private final AtomicLong responseProcessed = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong rejectedRequests = new AtomicLong(0);
    
    // 时间统计
    private final AtomicLong startTime = new AtomicLong(0);
    private final AtomicLong endTime = new AtomicLong(0);
    private final AtomicLong firstRequestTime = new AtomicLong(0);
    private final AtomicLong lastResponseTime = new AtomicLong(0);
    
    // 检测配置
    private final long maxWaitTimeMs;
    private final long checkIntervalMs;
    private final int stabilityThreshold;
    private final AtomicBoolean detectionActive = new AtomicBoolean(false);
    
    // 系统状态快照
    private volatile SystemStateSnapshot lastSnapshot;
    
    public SystemCompletionDetector(EnvoyServer service) {
        this(service, 120000, 100, 20); // 默认2分钟超时，100ms检查间隔，20次稳定检查
    }
    
    public SystemCompletionDetector(EnvoyServer service, long maxWaitTimeMs,
                                    long checkIntervalMs, int stabilityThreshold) {
        this.service = service;
        this.maxWaitTimeMs = maxWaitTimeMs;
        this.checkIntervalMs = checkIntervalMs;
        this.stabilityThreshold = stabilityThreshold;
        this.backpressureManagers = new CopyOnWriteArrayList<>();
    }
    
    /**
     * 添加背压管理器用于状态监控
     */
    public void addBackpressureManager(BackpressureManager manager) {
        backpressureManagers.add(manager);
    }
    
    /**
     * 开始检测
     */
    public void start() {
        long currentTime = System.nanoTime();
        startTime.set(currentTime);
        firstRequestTime.compareAndSet(0, currentTime);
        detectionActive.set(true);
        
        // 重置所有计数器
        sentRequests.set(0);
        accountProcessed.set(0);
        matchProcessed.set(0);
        responseProcessed.set(0);
        successfulRequests.set(0);
        rejectedRequests.set(0);
        
        System.out.println("🔍 系统完成状态检测器已启动");
    }
    
    /**
     * 记录发送的请求
     */
    public void recordSentRequest() {
        if (!detectionActive.get()) return;
        
        sentRequests.incrementAndGet();
        firstRequestTime.compareAndSet(0, System.nanoTime());
    }
    
    /**
     * 记录Account层处理完成
     */
    public void recordAccountProcessed() {
        if (!detectionActive.get()) return;
        accountProcessed.incrementAndGet();
    }
    
    /**
     * 记录Match层处理完成
     */
    public void recordMatchProcessed() {
        if (!detectionActive.get()) return;
        matchProcessed.incrementAndGet();
    }
    
    /**
     * 记录Response层处理完成（最终响应）
     */
    public void recordResponseProcessed(boolean isSuccess) {
        if (!detectionActive.get()) return;
        
        long responseTime = System.nanoTime();
        responseProcessed.incrementAndGet();
        
        if (isSuccess) {
            successfulRequests.incrementAndGet();
        } else {
            rejectedRequests.incrementAndGet();
        }
        
        lastResponseTime.set(responseTime);
    }
    
    /**
     * 等待系统完成所有处理
     * @return 完成检测结果
     */
    public SystemCompletionResult waitForSystemCompletion() throws InterruptedException {
        System.out.println("🕐 等待系统完成所有异步处理...");
        
        long startWaitTime = System.currentTimeMillis();
        int stableCount = 0;
        SystemStateSnapshot previousSnapshot = null;
        
        while (System.currentTimeMillis() - startWaitTime < maxWaitTimeMs) {
            SystemStateSnapshot currentSnapshot = captureSystemState();
            
            // 打印当前状态
            printSystemStatus(currentSnapshot);
            
            // 检查基本完成条件
            if (isBasicallyComplete(currentSnapshot)) {
                // 进行稳定性检测
                if (previousSnapshot != null && isStable(previousSnapshot, currentSnapshot)) {
                    stableCount++;
                    System.out.printf("🔍 稳定性检测: %d/%d 检查通过\n", stableCount, stabilityThreshold);
                    
                    if (stableCount >= stabilityThreshold) {
                        System.out.println("✅ 系统稳定，所有处理已完成！");
                        return createCompletionResult(true, currentSnapshot);
                    }
                } else {
                    stableCount = 0; // 重置稳定计数器
                }
            } else {
                stableCount = 0; // 重置稳定计数器
            }
            
            previousSnapshot = currentSnapshot;
            Thread.sleep(checkIntervalMs);
        }
        
        // 超时处理
        System.out.println("⚠️ 检测超时，强制完成");
        return createCompletionResult(false, captureSystemState());
    }
    
    /**
     * 捕获当前系统状态快照
     */
    private SystemStateSnapshot captureSystemState() {
        List<BackpressureManager.BackpressureStats> backpressureStats = 
            backpressureManagers.stream()
                .map(BackpressureManager::getStats)
                .toList();
        
        return new SystemStateSnapshot(
            System.currentTimeMillis(),
            sentRequests.get(),
            accountProcessed.get(),
            matchProcessed.get(),
            responseProcessed.get(),
            successfulRequests.get(),
            rejectedRequests.get(),
            backpressureStats
        );
    }
    
    /**
     * 检查是否基本完成（不考虑稳定性）
     */
    private boolean isBasicallyComplete(SystemStateSnapshot snapshot) {
        long totalSent = snapshot.sentRequests();
        long totalProcessed = snapshot.responseProcessed();
        
        // 基本完成条件：
        // 1. 已发送请求数 > 0
        // 2. 响应处理数 >= 发送数（考虑可能的重复或额外响应）
        // 3. 所有RingBuffer都不在高负载状态
        boolean basicComplete = totalSent > 0 && totalProcessed >= totalSent;
        boolean noHighLoad = snapshot.backpressureStats().stream()
            .noneMatch(BackpressureManager.BackpressureStats::isHighLoad);
        
        return basicComplete && noHighLoad;
    }
    
    /**
     * 检查两个快照之间的稳定性
     */
    private boolean isStable(SystemStateSnapshot prev, SystemStateSnapshot curr) {
        // 稳定性条件：所有关键指标在连续检查中无变化
        return prev.sentRequests() == curr.sentRequests() &&
               prev.accountProcessed() == curr.accountProcessed() &&
               prev.matchProcessed() == curr.matchProcessed() &&
               prev.responseProcessed() == curr.responseProcessed() &&
               prev.successfulRequests() == curr.successfulRequests() &&
               prev.rejectedRequests() == curr.rejectedRequests();
    }
    
    /**
     * 打印系统状态
     */
    private void printSystemStatus(SystemStateSnapshot snapshot) {
        long totalSent = snapshot.sentRequests();
        long totalProcessed = snapshot.responseProcessed();
        double completionRate = totalSent > 0 ? (double) totalProcessed / totalSent * 100 : 0;
        
        System.out.printf("📊 系统状态 - 发送: %d | 响应: %d (%.1f%%) | 成功: %d | 拒绝: %d\n",
            totalSent, totalProcessed, completionRate, 
            snapshot.successfulRequests(), snapshot.rejectedRequests());
        
        // 打印各层处理状态
        System.out.printf("📈 处理层级 - Account: %d | Match: %d | Response: %d\n",
            snapshot.accountProcessed(), snapshot.matchProcessed(), snapshot.responseProcessed());
        
        // 打印背压状态
        for (var stat : snapshot.backpressureStats()) {
            if (stat.isHighLoad()) {
                System.out.printf("⚠️ [%s] 高负载: %.1f%% 使用率\n", 
                    stat.name(), stat.currentUsageRate() * 100);
            }
        }
    }
    
    /**
     * 创建完成结果
     */
    private SystemCompletionResult createCompletionResult(boolean completedNormally, 
                                                         SystemStateSnapshot finalSnapshot) {
        endTime.set(System.nanoTime());
        detectionActive.set(false);
        
        long totalElapsedNs = endTime.get() - startTime.get();
        long processingElapsedNs = lastResponseTime.get() - firstRequestTime.get();
        
        return new SystemCompletionResult(
            completedNormally,
            finalSnapshot,
            totalElapsedNs,
            processingElapsedNs
        );
    }
    
    /**
     * 停止检测
     */
    public void stop() {
        detectionActive.set(false);
        endTime.set(System.nanoTime());
    }
    
    /**
     * 系统状态快照
     */
    public record SystemStateSnapshot(
        long timestamp,
        long sentRequests,
        long accountProcessed,
        long matchProcessed,
        long responseProcessed,
        long successfulRequests,
        long rejectedRequests,
        List<BackpressureManager.BackpressureStats> backpressureStats
    ) {}
    
    /**
     * 系统完成检测结果
     */
    public record SystemCompletionResult(
        boolean completedNormally,
        SystemStateSnapshot finalState,
        long totalElapsedNs,
        long processingElapsedNs
    ) {
        
        public void printDetailedSummary() {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("📋 系统完成状态检测报告");
            System.out.println("=".repeat(60));
            
            String status = completedNormally ? "✅ 正常完成" : "⚠️ 超时完成";
            System.out.printf("🏁 完成状态: %s\n", status);
            
            System.out.printf("📊 请求统计: 发送=%d, 最终响应=%d\n", 
                finalState.sentRequests(), finalState.responseProcessed());
            
            System.out.printf("📈 处理层级: Account=%d, Match=%d, Response=%d\n",
                finalState.accountProcessed(), finalState.matchProcessed(), finalState.responseProcessed());
            
            System.out.printf("✅ 成功处理: %d (%.1f%%)\n", 
                finalState.successfulRequests(),
                finalState.sentRequests() > 0 ? (double)finalState.successfulRequests() / finalState.sentRequests() * 100 : 0);
            
            System.out.printf("❌ 拒绝处理: %d (%.1f%%)\n",
                finalState.rejectedRequests(),
                finalState.sentRequests() > 0 ? (double)finalState.rejectedRequests() / finalState.sentRequests() * 100 : 0);
            
            double totalElapsedMs = totalElapsedNs / 1_000_000.0;
            double processingElapsedMs = processingElapsedNs / 1_000_000.0;
            
            System.out.printf("⏱️ 总耗时: %.2f ms\n", totalElapsedMs);
            System.out.printf("⏱️ 处理耗时: %.2f ms\n", processingElapsedMs);
            
            if (finalState.responseProcessed() > 0) {
                double avgProcessingTime = processingElapsedMs / finalState.responseProcessed();
                double throughput = finalState.responseProcessed() / (processingElapsedMs / 1000.0);
                
                System.out.printf("⚡ 平均处理时间: %.3f ms\n", avgProcessingTime);
                System.out.printf("🚀 系统吞吐量: %.0f requests/sec\n", throughput);
            }
            
            System.out.println("=".repeat(60));
        }
    }
} 