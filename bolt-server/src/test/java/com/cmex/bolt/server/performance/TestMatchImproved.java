package com.cmex.bolt.server.performance;

import com.cmex.bolt.server.grpc.SpotServiceImpl;
import com.cmex.bolt.server.grpc.Bolt;
import com.cmex.bolt.server.util.CompletionTracker;
import com.cmex.bolt.server.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.server.grpc.Bolt.PlaceOrderRequest;
import static com.cmex.bolt.server.util.SpotServiceUtil.*;

/**
 * 使用CompletionTracker的改进版TestMatch
 * 演示如何在有背压控制的情况下正确判断处理完成
 */
public class TestMatchImproved {
    private static final SpotServiceImpl service = new SpotServiceImpl();
    private static final int TIMES = 100000;

    @BeforeAll
    public static void init() {
        increase(service, 1, 1, String.valueOf(TIMES));
        increase(service, 2, 2, String.valueOf(TIMES));
        increase(service, 3, 1, String.valueOf(TIMES));
        increase(service, 4, 3, String.valueOf(TIMES));
    }

    @Test
    public void testMatchWithCompletionTracking() throws InterruptedException {
        // 创建完成跟踪器
        CompletionTracker tracker = new CompletionTracker(service);
        
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(4);

        System.out.println("=== Starting TestMatch with Completion Tracking ===");
        
        // 开始性能统计
        tracker.start();
        
        // 启动4个线程发送订单
        executor.submit(() -> sendOrdersWithTracking(1, 1, PlaceOrderRequest.Side.BID, tracker, latch));
        executor.submit(() -> sendOrdersWithTracking(1, 2, PlaceOrderRequest.Side.ASK, tracker, latch));
        executor.submit(() -> sendOrdersWithTracking(2, 3, PlaceOrderRequest.Side.BID, tracker, latch));
        executor.submit(() -> sendOrdersWithTracking(2, 4, PlaceOrderRequest.Side.ASK, tracker, latch));

        // 等待所有发送线程完成
        latch.await();
        long sendElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf("All orders sent in %d ms\n", sendElapsed);

        // 等待处理完成
        CompletionTracker.CompletionResult result = tracker.waitForCompletion();
        long totalElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // 打印基本结果
        result.printSummary();
        System.out.printf("Total elapsed time: %d ms\n", totalElapsed);
        
        // 打印详细的性能统计
        result.printPerformanceStats();
        
        // 验证结果
        validateResults(result);

        executor.shutdown();
    }

    /**
     * 发送订单并跟踪完成状态
     */
    private void sendOrdersWithTracking(int symbolId, int accountId, PlaceOrderRequest.Side side, 
                                       CompletionTracker tracker, CountDownLatch latch) {
        try {
            System.out.printf("Thread %s starting to send %s orders\n", 
                    Thread.currentThread().getName(), side);
            
            for (int i = 1; i <= TIMES; i++) {
                tracker.recordSentRequest();
                
                // 发送订单并设置回调跟踪
                final int currentIteration = i; // 创建final变量用于lambda
                service.placeOrder(PlaceOrderRequest.newBuilder()
                        .setRequestId(1)
                        .setSymbolId(symbolId)
                        .setAccountId(accountId)
                        .setType(PlaceOrderRequest.Type.LIMIT)
                        .setSide(side)
                        .setPrice("1")
                        .setQuantity("1")
                        .build(), new FakeStreamObserver<Bolt.PlaceOrderResponse>(response -> {
                            // 根据响应类型记录结果
                            if (response.getCode() == 1) {
                                tracker.recordSuccessfulRequest();
                            } else {
                                tracker.recordRejectedRequest();
                                // 可选：打印拒绝原因
                                if (currentIteration % 1000 == 0) {
                                    System.out.printf("Request rejected: %s\n", response.getMessage());
                                }
                            }
                        }));
                
                // 每1000个请求打印一次进度
                if (i % 1000 == 0) {
                    System.out.printf("%s: sent %d orders\n", side, i);
                }
            }
            
            System.out.printf("Thread %s completed sending %s orders\n", 
                    Thread.currentThread().getName(), side);
        } finally {
            latch.countDown();
        }
    }

    /**
     * 验证测试结果
     */
    private void validateResults(CompletionTracker.CompletionResult result) {
        System.out.println("\n=== Result Validation ===");
        
        if (result.deadlocked()) {
            System.out.println("❌ Test failed due to system deadlock");
            System.out.println("Recommendation: Increase RingBuffer capacity or fix circular dependencies");
            return;
        }
        
        if (!result.completed()) {
            System.out.println("⚠️ Test completed with timeout");
            System.out.println("Some requests may still be processing");
        }
        
        CompletionTracker.CompletionStats stats = result.stats();
        
        // 检查完成率
        if (stats.getCompletionRate() >= 0.95) { // 95%完成率
            System.out.println("✅ Excellent completion rate");
        } else if (stats.getCompletionRate() >= 0.8) { // 80%完成率
            System.out.println("⚠️ Acceptable completion rate, but system is under stress");
        } else {
            System.out.println("❌ Poor completion rate, system may be overloaded");
        }
        
        // 检查成功率（在已处理的请求中）
        double successRateOfProcessed = stats.processedRequests() > 0 ? 
                (double) stats.successfulRequests() / stats.processedRequests() : 0;
        
        if (successRateOfProcessed >= 0.9) {
            System.out.println("✅ High success rate among processed requests");
        } else {
            System.out.println("⚠️ Low success rate, check business logic");
        }
        
        // 提供优化建议
        if (stats.getRejectionRate() > 0.2) { // 拒绝率超过20%
            System.out.println("\n💡 Optimization suggestions:");
            System.out.println("- Increase RingBuffer capacity");
            System.out.println("- Reduce request sending rate");
            System.out.println("- Add request queuing/retry mechanism");
        }
        
        // 打印系统统计
        try {
            var systemStats = service.getMonitorSummary();
            System.out.printf("Final system rejection rate: %.2f%%\n", 
                    systemStats.getRejectionRate() * 100);
        } catch (Exception e) {
            System.out.println("Unable to get system stats: " + e.getMessage());
        }
    }
} 