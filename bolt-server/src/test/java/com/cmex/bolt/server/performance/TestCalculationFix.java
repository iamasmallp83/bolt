package com.cmex.bolt.server.performance;

import com.cmex.bolt.server.grpc.SpotServiceImpl;
import com.cmex.bolt.server.grpc.Bolt;
import com.cmex.bolt.server.util.CompletionTracker;
import com.cmex.bolt.server.util.FakeStreamObserver;
import com.cmex.bolt.server.util.HTMLReportGenerator;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.server.grpc.Bolt.PlaceOrderRequest;
import static com.cmex.bolt.server.util.SpotServiceUtil.increase;

/**
 * 测试平均处理时间计算修复
 */
public class TestCalculationFix {
    private static final SpotServiceImpl service = new SpotServiceImpl();

    @Test
    public void testAverageProcessingTimeCalculation() throws InterruptedException, IOException {
        // 预充值
        increase(service, 1, 1, "10000");
        increase(service, 2, 2, "10000");

        // 创建跟踪器和HTML报告生成器
        CompletionTracker tracker = new CompletionTracker(service);
        HTMLReportGenerator reportGenerator = new HTMLReportGenerator("平均处理时间计算验证报告");
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(2);

        System.out.println("=== Testing Average Processing Time Calculation ===");
        
        // 开始统计
        tracker.start();
        
        int orderCount = 1000; // 测试1000个订单
        
        // 启动2个线程
        executor.submit(() -> sendTestOrders(1, 1, PlaceOrderRequest.Side.BID, orderCount, tracker, latch));
        executor.submit(() -> sendTestOrders(1, 2, PlaceOrderRequest.Side.ASK, orderCount, tracker, latch));

        // 等待发送完成
        latch.await();
        long sendElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf("发送完成耗时: %d ms\n", sendElapsed);

        // 等待处理完成
        CompletionTracker.CompletionResult result = tracker.waitForCompletion();
        long totalElapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // 验证计算结果
        CompletionTracker.CompletionStats stats = result.stats();
        
        System.out.println("\n=== 计算验证 ===");
        System.out.printf("发送请求数: %d\n", stats.sentRequests());
        System.out.printf("处理请求数: %d\n", stats.processedRequests());
        System.out.printf("总处理耗时: %.2f ms\n", stats.getProcessingElapsedMs());
        System.out.printf("平均处理时间: %.6f ms\n", stats.getAverageProcessingTimeMs());
        
        // 手动验证计算
        double expectedAvg = stats.getProcessingElapsedMs() / stats.processedRequests();
        System.out.printf("手动计算平均时间: %.6f ms\n", expectedAvg);
        System.out.printf("计算误差: %.9f ms\n", Math.abs(expectedAvg - stats.getAverageProcessingTimeMs()));
        
        // 合理性检查
        if (stats.getAverageProcessingTimeMs() > stats.getProcessingElapsedMs()) {
            System.out.println("❌ 错误：平均处理时间不能大于总处理时间！");
        } else {
            System.out.println("✅ 计算结果合理");
        }
        
        // 打印详细统计
        result.printPerformanceStats();

        // 添加到HTML报告并生成
        reportGenerator.addTestResult(
            "平均处理时间计算验证", 
            result, 
            sendElapsed, 
            "验证修复后的平均处理时间计算公式是否正确"
        );
        
        String reportPath = "calculation-fix-report-" + System.currentTimeMillis() + ".html";
        reportGenerator.generateReport(reportPath);
        
        System.out.println("\n📄 验证报告已生成: " + reportPath);

        executor.shutdown();
    }

    private void sendTestOrders(int symbolId, int accountId, PlaceOrderRequest.Side side, 
                               int orderCount, CompletionTracker tracker, CountDownLatch latch) {
        try {
            for (int i = 1; i <= orderCount; i++) {
                tracker.recordSentRequest();
                
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
            }
            System.out.printf("%s 方向发送 %d 个订单完成\n", side, orderCount);
        } finally {
            latch.countDown();
        }
    }
} 