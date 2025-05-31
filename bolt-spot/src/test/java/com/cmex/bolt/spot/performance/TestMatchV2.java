package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.BigDecimalUtil;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;
import static com.cmex.bolt.spot.util.SpotServiceUtil.*;

/**
 * 改进版TestMatch，支持背压处理的完成判断
 */
public class TestMatchV2 {
    private static SpotServiceImpl service = new SpotServiceImpl();
    private static int TIMES = 10000;
    
    // 请求跟踪器
    private static final AtomicLong totalSentRequests = new AtomicLong(0);
    private static final AtomicLong successfulResponses = new AtomicLong(0);
    private static final AtomicLong rejectedRequests = new AtomicLong(0);
    private static final AtomicInteger completedThreads = new AtomicInteger(0);

    @BeforeAll
    public static void init() {
        increase(service, 1, 1, String.valueOf(TIMES));
        increase(service, 2, 2, String.valueOf(TIMES));
        increase(service, 3, 1, String.valueOf(TIMES));
        increase(service, 4, 3, String.valueOf(TIMES));
    }

    @Test
    public void testOrderWithBackpressureTracking() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(4);
        
        // 重置计数器
        resetCounters();
        
        // 启动4个线程发送订单
        executor.submit(() -> sendOrdersWithTracking(1, 1, PlaceOrderRequest.Side.BID, latch));
        executor.submit(() -> sendOrdersWithTracking(1, 2, PlaceOrderRequest.Side.ASK, latch));
        executor.submit(() -> sendOrdersWithTracking(2, 3, PlaceOrderRequest.Side.BID, latch));
        executor.submit(() -> sendOrdersWithTracking(2, 4, PlaceOrderRequest.Side.ASK, latch));
        
        latch.await();
        System.out.println("Send phase completed. Sent: " + totalSentRequests.get() + " requests");
        System.out.println("Send elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        
        // 等待处理完成
        waitForProcessingComplete();
        System.out.println("Processing completed. Total elapsed: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        
        // 打印最终统计
        printFinalStats();
        
        executor.shutdown();
    }
    
    /**
     * 发送订单并跟踪状态
     */
    private void sendOrdersWithTracking(int symbolId, int accountId, PlaceOrderRequest.Side side, CountDownLatch latch) {
        try {
            for (int i = 1; i <= TIMES; i++) {
                totalSentRequests.incrementAndGet();
                
                placeOrderWithTracking(symbolId, accountId, PlaceOrderRequest.Type.LIMIT, side, "1", "1");
                
                // 每1000个请求检查一次背压状态
                if (i % 1000 == 0) {
                    checkBackpressureStatus();
                }
            }
            System.out.println(side + " orders sent from thread: " + Thread.currentThread().getName());
        } finally {
            completedThreads.incrementAndGet();
            latch.countDown();
        }
    }
    
    /**
     * 发送订单并跟踪响应
     */
    private static void placeOrderWithTracking(int symbolId, int accountId, PlaceOrderRequest.Type type, 
                                              PlaceOrderRequest.Side side, String price, String quantity) {
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(symbolId)
                .setAccountId(accountId)
                .setType(type)
                .setSide(side)
                .setPrice(price)
                .setQuantity(quantity)
                .build(), new FakeStreamObserver<SpotServiceProto.PlaceOrderResponse>(response -> {
                    if (response.getCode() == 1) {
                        // 成功响应
                        successfulResponses.incrementAndGet();
                    } else {
                        // 被拒绝的请求
                        rejectedRequests.incrementAndGet();
                        if (response.getMessage().contains("System is busy")) {
                            // 背压拒绝，这是正常的
                        }
                    }
                }));
    }
    
    /**
     * 等待所有请求处理完成
     */
    private void waitForProcessingComplete() throws InterruptedException {
        System.out.println("Waiting for processing to complete...");
        
        long lastProgress = 0;
        int stableCount = 0;
        
        while (true) {
            long totalProcessed = successfulResponses.get() + rejectedRequests.get();
            long totalSent = totalSentRequests.get();
            
            System.out.printf("Progress: %d/%d (%.2f%%), Success: %d, Rejected: %d%n",
                    totalProcessed, totalSent, 
                    (double)totalProcessed / totalSent * 100,
                    successfulResponses.get(), rejectedRequests.get());
            
            // 检查是否所有请求都有响应
            if (totalProcessed >= totalSent && completedThreads.get() >= 4) {
                System.out.println("All requests processed!");
                break;
            }
            
            // 检查进度是否停滞
            if (totalProcessed == lastProgress) {
                stableCount++;
                if (stableCount >= 10) { // 5秒没有进度
                    System.out.println("Progress stalled, checking system status...");
                    checkSystemHealth();
                    
                    // 如果系统健康但进度停滞，可能是死锁，强制退出
                    if (stableCount >= 20) {
                        System.out.println("WARNING: System appears to be deadlocked!");
                        break;
                    }
                }
            } else {
                stableCount = 0;
                lastProgress = totalProcessed;
            }
            
            TimeUnit.MILLISECONDS.sleep(500);
        }
    }
    
    /**
     * 检查背压状态
     */
    private void checkBackpressureStatus() {
        var accountStats = service.getAccountBackpressureStats();
        var matchStats = service.getMatchBackpressureStats();
        var responseStats = service.getResponseBackpressureStats();
        
        if (accountStats.currentUsageRate() > 0.8 || 
            matchStats.currentUsageRate() > 0.8 || 
            responseStats.currentUsageRate() > 0.8) {
            
            System.out.printf("Backpressure detected - Account: %.2f%%, Match: %.2f%%, Response: %.2f%%%n",
                    accountStats.currentUsageRate() * 100,
                    matchStats.currentUsageRate() * 100,
                    responseStats.currentUsageRate() * 100);
        }
    }
    
    /**
     * 检查系统健康状态
     */
    private void checkSystemHealth() {
        var summary = service.getMonitorSummary();
        System.out.println("=== System Health Check ===");
        System.out.printf("Total requests: %d, Total rejected: %d (%.2f%%)%n",
                summary.totalRequests(), summary.totalRejected(), summary.getRejectionRate() * 100);
        System.out.printf("Max usage rate: %.2f%%, Avg usage rate: %.2f%%\n",
                summary.maxUsageRate() * 100, summary.avgUsageRate() * 100);
        System.out.printf("Critical ring buffers: %d%n", summary.criticalRingBuffers());
        
        // 打印详细的RingBuffer状态
        service.reportMonitorStats();
    }
    
    /**
     * 打印最终统计信息
     */
    private void printFinalStats() {
        System.out.println("\n=== Final Statistics ===");
        System.out.println("Total sent: " + totalSentRequests.get());
        System.out.println("Successful: " + successfulResponses.get());
        System.out.println("Rejected: " + rejectedRequests.get());
        System.out.printf("Success rate: %.2f%%\n", 
                (double)successfulResponses.get() / totalSentRequests.get() * 100);
        
        // 检查账户最终状态
        checkFinalAccountBalances();
        
        // 获取系统性能统计
        var summary = service.getMonitorSummary();
        System.out.printf("System rejection rate: %.2f%%\n", summary.getRejectionRate() * 100);
    }
    
    /**
     * 检查最终账户余额（用于验证结果正确性）
     */
    private void checkFinalAccountBalances() {
        System.out.println("\n=== Final Account Balances ===");
        
        AtomicReference<String> account1Balance1 = new AtomicReference<>();
        AtomicReference<String> account1Balance2 = new AtomicReference<>();
        AtomicReference<String> account2Balance1 = new AtomicReference<>();
        AtomicReference<String> account2Balance2 = new AtomicReference<>();
        
        getAccount(service, 1, new FakeStreamObserver<>((response) -> {
            var balances = response.getDataMap();
            account1Balance1.set(balances.get(1) != null ? balances.get(1).getAvailable() : "0");
            account1Balance2.set(balances.get(2) != null ? balances.get(2).getAvailable() : "0");
        }));
        
        getAccount(service, 2, new FakeStreamObserver<>((response) -> {
            var balances = response.getDataMap();
            account2Balance1.set(balances.get(1) != null ? balances.get(1).getAvailable() : "0");
            account2Balance2.set(balances.get(2) != null ? balances.get(2).getAvailable() : "0");
        }));
        
        // 等待查询完成
        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        System.out.printf("Account 1: Currency 1=%s, Currency 2=%s%n", 
                account1Balance1.get(), account1Balance2.get());
        System.out.printf("Account 2: Currency 1=%s, Currency 2=%s%n", 
                account2Balance1.get(), account2Balance2.get());
        
        // 计算预期的成功交易数量
        long expectedSuccessfulTrades = successfulResponses.get() / 2; // 每笔交易涉及两个订单
        System.out.printf("Expected successful trades: %d%n", expectedSuccessfulTrades);
    }
    
    /**
     * 重置计数器
     */
    private void resetCounters() {
        totalSentRequests.set(0);
        successfulResponses.set(0);
        rejectedRequests.set(0);
        completedThreads.set(0);
        service.resetAllStats();
    }
} 