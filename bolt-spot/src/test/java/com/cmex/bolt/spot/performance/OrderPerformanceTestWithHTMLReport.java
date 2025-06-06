package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.CompletionTracker;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.cmex.bolt.spot.util.HTMLReportGenerator;
import com.cmex.bolt.spot.util.MemoryAnalyzer;
import com.cmex.bolt.spot.util.MemoryLeakDetector;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;
import static com.cmex.bolt.spot.util.SpotServiceUtil.increase;

/**
 * 订单性能测试 - 带HTML报告生成
 * 全面测试系统在不同负载下的性能表现
 */
public class OrderPerformanceTestWithHTMLReport {
    private static final SpotServiceImpl service = new SpotServiceImpl();

    @Test
    public void runComprehensivePerformanceTest() throws InterruptedException, IOException {
        // 创建HTML报告生成器
        HTMLReportGenerator reportGenerator = new HTMLReportGenerator("高频交易系统性能测试报告");
        
        System.out.println("🚀 开始全面性能测试...");
        System.out.println("📊 将生成详细的HTML性能报告");
        
        // 初始内存分析
        MemoryAnalyzer.analyzeMemoryUsage("TEST_START");
        
        // 预充值账户
        increase(service, 1, 1, "100000");
        increase(service, 1, 2, "100000");
        increase(service, 2, 1, "100000");
        increase(service, 2, 2, "100000");

        // 测试1: 轻负载 - 1,000订单
        runLoadTest(reportGenerator, "轻负载测试", 1000, 2, 
                   "测试系统在轻负载下的基准性能");

        // 强制垃圾回收和内存分析
        System.gc();
        Thread.sleep(3000); // 给GC更多时间
        MemoryAnalyzer.analyzeMemoryUsage("AFTER_LIGHT_LOAD_GC");

        // 测试2: 中等负载 - 10,000订单
        runLoadTest(reportGenerator, "中等负载测试", 10000, 4, 
                   "测试系统在中等负载下的性能表现");

        // 强制垃圾回收
        System.gc();
        Thread.sleep(3000);

        // 测试3: 高负载 - 50,000订单
        runLoadTest(reportGenerator, "高负载测试", 50000, 8, 
                   "测试系统在高负载下的性能极限");

        // 强制垃圾回收
        System.gc();
        Thread.sleep(3000);

        // 测试4: 极限负载 - 100,000订单
        runLoadTest(reportGenerator, "极限负载测试", 100000, 10, 
                   "测试系统在极限负载下的稳定性和性能");

        // 生成HTML报告
        String reportPath = "performance-report-" + System.currentTimeMillis() + ".html";
        reportGenerator.generateReport(reportPath);
        
        System.out.println("\n🎉 性能测试完成！");
        System.out.println("📄 详细报告已保存到: " + reportPath);
        System.out.println("💡 请在浏览器中打开查看详细的性能分析和图表");
    }

    private void runLoadTest(HTMLReportGenerator reportGenerator, String testName, 
                           int orderCount, int threadCount, String description) 
                           throws InterruptedException {
        
        System.out.println("\n" + "=".repeat(60));
        System.out.printf("🧪 开始 %s (%,d 订单, %d 线程)%n", testName, orderCount, threadCount);
        System.out.println("=".repeat(60));

        CompletionTracker tracker = new CompletionTracker(service);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(threadCount);

        // 开始统计
        tracker.start();

        // 计算每个线程的订单数
        int ordersPerThread = orderCount / threadCount;
        int remainingOrders = orderCount % threadCount;

        // 启动多个线程发送订单
        for (int i = 0; i < threadCount; i++) {
            int currentThreadOrders = ordersPerThread + (i < remainingOrders ? 1 : 0);
            int threadId = i;
            
            executor.submit(() -> {
                sendTestOrders(threadId, currentThreadOrders, tracker, latch);
            });
        }

        // 等待发送完成
        latch.await();
        long sendElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf("📤 订单发送完成，耗时: %d ms%n", sendElapsed);

        // 等待处理完成
        CompletionTracker.CompletionResult result = tracker.waitForCompletion();
        long totalElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // 显示详细结果
        result.printSummary();
        result.printPerformanceStats();

        // 添加到HTML报告
        reportGenerator.addTestResult(testName, result, sendElapsed, description);

        System.out.printf("🏁 %s 完成，总耗时: %d ms%n", testName, totalElapsed);

        executor.shutdown();
    }

    private void sendTestOrders(int threadId, int orderCount, 
                               CompletionTracker tracker, CountDownLatch latch) {
        try {
            // 交替发送买卖订单
            for (int i = 1; i <= orderCount; i++) {
                tracker.recordSentRequest();
                
                // 根据线程ID和订单序号确定交易对和账户
                int symbolId = (threadId % 2) + 1;  // 1 或 2
                int accountId = ((i % 2) == 1) ? 1 : 2;  // 交替使用账户1和2
                PlaceOrderRequest.Side side = ((i % 2) == 1) ? 
                    PlaceOrderRequest.Side.BID : PlaceOrderRequest.Side.ASK;

                service.placeOrder(PlaceOrderRequest.newBuilder()
                        .setRequestId(threadId * 1000000 + i)  // 确保唯一ID
                        .setSymbolId(symbolId)
                        .setAccountId(accountId)
                        .setType(PlaceOrderRequest.Type.LIMIT)
                        .setSide(side)
                        .setPrice("1.0")
                        .setQuantity("1.0")
                        .build(), new FakeStreamObserver<SpotServiceProto.PlaceOrderResponse>(response -> {
                            if (response.getCode() == 1) {
                                tracker.recordSuccessfulRequest();
                            } else {
                                tracker.recordRejectedRequest();
                            }
                        }));

                // 轻微延迟避免过度饱和
                if (i % 1000 == 0) {
                    Thread.sleep(1);
                }
            }
            
            System.out.printf("🧵 线程 %d 完成 %,d 个订单发送%n", threadId, orderCount);
            
        } catch (Exception e) {
            System.err.printf("❌ 线程 %d 发生错误: %s%n", threadId, e.getMessage());
        } finally {
            latch.countDown();
        }
    }
} 