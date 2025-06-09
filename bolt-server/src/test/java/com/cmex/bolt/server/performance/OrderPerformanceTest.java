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
 * 订单处理性能测试
 * 专门用于测试和分析订单处理的效率统计
 */
public class OrderPerformanceTest {
    private static final SpotServiceImpl service = new SpotServiceImpl();

    @BeforeAll
    public static void init() {
        // 为每个账户预充值足够的资金
        increase(service, 1, 1, "1000000");  // 账户1 - 币种1
        increase(service, 2, 2, "1000000");  // 账户2 - 币种2
        increase(service, 3, 1, "1000000");  // 账户3 - 币种1
        increase(service, 4, 3, "1000000");  // 账户4 - 币种3
    }

    @Test
    public void testLowVolumePerformance() throws InterruptedException {
        System.out.println("\n🚀 === Low Volume Performance Test (1K orders) ===");
        runPerformanceTest(1000, "Low Volume");
    }

    @Test 
    public void testMediumVolumePerformance() throws InterruptedException {
        System.out.println("\n🚀 === Medium Volume Performance Test (10K orders) ===");
        runPerformanceTest(10000, "Medium Volume");
    }

    @Test
    public void testHighVolumePerformance() throws InterruptedException {
        System.out.println("\n🚀 === High Volume Performance Test (100K orders) ===");
        runPerformanceTest(100000, "High Volume");
    }

    /**
     * 运行性能测试
     */
    private void runPerformanceTest(int orderCount, String testName) throws InterruptedException {
        // 创建性能跟踪器
        CompletionTracker tracker = new CompletionTracker(service, 
                120000, // 最多等待2分钟
                1000,   // 每秒检查一次
                30);    // 连续30秒无变化认为完成

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(2); // 只启动2个线程（买卖各一个）

        System.out.printf("Target orders: %d (%d per side)\n", orderCount * 2, orderCount);
        
        // 开始性能统计
        tracker.start();
        
        // 启动买卖双方线程
        executor.submit(() -> sendOrdersForPerformanceTest(1, 1, PlaceOrderRequest.Side.BID, 
                orderCount, tracker, latch, "BUY"));
        executor.submit(() -> sendOrdersForPerformanceTest(1, 2, PlaceOrderRequest.Side.ASK, 
                orderCount, tracker, latch, "SELL"));

        // 等待发送完成
        latch.await();
        long sendElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf("📤 Order sending completed in: %.2f ms\n", (double)sendElapsed);

        // 等待处理完成
        System.out.println("⏳ Waiting for order processing to complete...");
        CompletionTracker.CompletionResult result = tracker.waitForCompletion();
        long totalElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // 打印测试结果
        System.out.println("\n" + "=".repeat(60));
        System.out.printf("🏁 %s Test Results\n", testName);
        System.out.println("=".repeat(60));
        
        // 基本完成统计
        result.printSummary();
        
        // 详细性能统计
        result.printPerformanceStats();
        
        // 额外的分析
        printDetailedAnalysis(result, sendElapsed, totalElapsed, orderCount * 2);
        
        // 系统健康检查
        printSystemHealthSummary();

        executor.shutdown();
    }

    /**
     * 发送订单用于性能测试
     */
    private void sendOrdersForPerformanceTest(int symbolId, int accountId, PlaceOrderRequest.Side side, 
                                            int orderCount, CompletionTracker tracker, 
                                            CountDownLatch latch, String sideDescription) {
        try {
            System.out.printf("🔄 Thread %s: Starting to send %d %s orders\n", 
                    Thread.currentThread().getName(), orderCount, sideDescription);
            
            long startTime = System.nanoTime();
            
            for (int i = 1; i <= orderCount; i++) {
                tracker.recordSentRequest();
                
                // 发送订单
                service.placeOrder(PlaceOrderRequest.newBuilder()
                        .setRequestId(i)
                        .setSymbolId(symbolId)
                        .setAccountId(accountId)
                        .setType(PlaceOrderRequest.Type.LIMIT)
                        .setSide(side)
                        .setPrice("1.0")
                        .setQuantity("1.0")
                        .build(), new FakeStreamObserver<Bolt.PlaceOrderResponse>(response -> {
                            if (response.getCode() == 1) {
                                tracker.recordSuccessfulRequest();
                            } else {
                                tracker.recordRejectedRequest();
                            }
                        }));
                
                // 定期报告进度
                if (i % 1000 == 0 || i == orderCount) {
                    long elapsed = (System.nanoTime() - startTime) / 1_000_000;
                    double rate = elapsed > 0 ? (double) i / elapsed * 1000 : 0;
                    System.out.printf("📊 %s: sent %d/%d orders (%.1f req/s)\n", 
                            sideDescription, i, orderCount, rate);
                }
            }
            
            long sendTime = (System.nanoTime() - startTime) / 1_000_000;
            System.out.printf("✅ %s: Completed sending %d orders in %.2f ms\n", 
                    sideDescription, orderCount, (double)sendTime);
                    
        } finally {
            latch.countDown();
        }
    }

    /**
     * 打印详细分析
     */
    private void printDetailedAnalysis(CompletionTracker.CompletionResult result, 
                                     long sendElapsedMs, long totalElapsedMs, int expectedOrders) {
        System.out.println("\n📈 === Detailed Analysis ===");
        
        CompletionTracker.CompletionStats stats = result.stats();
        
        // 发送速率分析
        double sendRate = sendElapsedMs > 0 ? (double) expectedOrders / sendElapsedMs * 1000 : 0;
        System.out.printf("📤 Order Sending Rate: %.2f orders/sec\n", sendRate);
        
        // 处理延迟分析
        double processingDelay = stats.getProcessingElapsedMs() - sendElapsedMs;
        System.out.printf("⏱️ Processing Delay: %.2f ms\n", processingDelay);
        
        // 匹配率分析
        double matchRate = stats.successfulRequests() > 0 ? 
                (double) stats.successfulRequests() / 2 / stats.successfulRequests() * 100 : 0;
        System.out.printf("🎯 Estimated Match Rate: %.2f%% (assuming pair matching)\n", matchRate);
        
        // 系统效率分析
        double systemEfficiency = stats.getSuccessRate() * stats.getCompletionRate();
        System.out.printf("🏭 System Efficiency: %.2f%% (success × completion)\n", systemEfficiency * 100);
        
        // 资源利用率评估
        if (stats.getRejectionRate() > 0.1) {
            System.out.printf("⚠️ High rejection rate (%.2f%%) indicates system overload\n", 
                    stats.getRejectionRate() * 100);
        } else {
            System.out.println("✅ Low rejection rate indicates good system capacity");
        }
    }

    /**
     * 打印系统健康摘要
     */
    private void printSystemHealthSummary() {
        try {
            System.out.println("\n🔍 === System Health Summary ===");
            
            var accountStats = service.getAccountBackpressureStats();
            var matchStats = service.getMatchBackpressureStats();
            var responseStats = service.getResponseBackpressureStats();
            
            System.out.printf("🏦 Account RingBuffer: %.1f%% usage, %.2f%% rejection rate\n",
                    accountStats.currentUsageRate() * 100, accountStats.getRejectionRate() * 100);
            System.out.printf("⚖️ Match RingBuffer: %.1f%% usage, %.2f%% rejection rate\n",
                    matchStats.currentUsageRate() * 100, matchStats.getRejectionRate() * 100);
            System.out.printf("📬 Response RingBuffer: %.1f%% usage, %.2f%% rejection rate\n",
                    responseStats.currentUsageRate() * 100, responseStats.getRejectionRate() * 100);
            
            var summary = service.getMonitorSummary();
            System.out.printf("🎯 Overall System: %.2f%% rejection rate, %d critical buffers\n",
                    summary.getRejectionRate() * 100, summary.criticalRingBuffers());
                    
        } catch (Exception e) {
            System.out.println("❌ Unable to get system health stats: " + e.getMessage());
        }
    }
} 