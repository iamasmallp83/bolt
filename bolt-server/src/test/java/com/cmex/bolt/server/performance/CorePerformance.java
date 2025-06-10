package com.cmex.bolt.server.performance;

import com.cmex.bolt.server.grpc.Envoy.PlaceOrderRequest;
import com.cmex.bolt.server.grpc.Envoy.PlaceOrderResponse;
import com.cmex.bolt.server.grpc.EnvoyServer;
import com.cmex.bolt.server.util.SystemCompletionDetector;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 增强版订单性能测试
 * 使用SystemCompletionDetector来准确判断复杂异步系统的完成状态
 */
public class CorePerformance {

    private EnvoyServer service;
    private SystemCompletionDetector detector;

    @BeforeEach
    void setUp() {
        service = new EnvoyServer();
        detector = new SystemCompletionDetector(service, 60000, 50, 15);

        // 注册背压管理器（假设可以通过某种方式获取）
        // detector.addBackpressureManager(service.getAccountBackpressureManager());
        // detector.addBackpressureManager(service.getMatchBackpressureManager());
        // detector.addBackpressureManager(service.getResponseBackpressureManager());
    }

    @Test
    public void testSystemCompletionDetection() throws Exception {
        System.out.println("🚀 开始增强版性能测试（精确完成状态检测）");

        // 测试参数
        int orderCount = 10000;
        int threadCount = 4;

        // 启动系统完成状态检测器
        detector.start();

        // 创建线程池发送订单
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger completedThreads = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        // 分批发送订单
        for (int threadId = 0; threadId < threadCount; threadId++) {
            final int tid = threadId;
            executor.submit(() -> {
                int ordersPerThread = orderCount / threadCount;
                int startOrderId = tid * ordersPerThread;

                for (int i = 0; i < ordersPerThread; i++) {
                    sendOrderWithDetection(startOrderId + i + 1);
                }

                System.out.printf("🧵 线程 %d 完成 %d 个订单发送\n", tid, ordersPerThread);
                completedThreads.incrementAndGet();
            });
        }

        // 等待所有发送线程完成
        while (completedThreads.get() < threadCount) {
            Thread.sleep(10);
        }

        long sendCompleteTime = System.currentTimeMillis();
        System.out.printf("📤 所有订单发送完成，耗时: %d ms\n", sendCompleteTime - startTime);

        // 等待系统完成所有异步处理
        SystemCompletionDetector.SystemCompletionResult result = detector.waitForSystemCompletion();

        // 打印详细结果
        result.printDetailedSummary();

        // 关闭线程池
        executor.shutdown();

        // 验证结果
        assert result.completedNormally() : "系统未能正常完成所有处理";
        assert result.finalState().sentRequests() == orderCount : "发送订单数不匹配";

        System.out.println("✅ 增强版性能测试完成！");
    }

    /**
     * 发送订单并记录到检测器
     */
    private void sendOrderWithDetection(int orderSequence) {
        // 构建订单请求
        PlaceOrderRequest request = PlaceOrderRequest.newBuilder()
                .setAccountId(((orderSequence % 2) == 1) ? 1 : 2)
                .setSymbolId(1)
                .setSide(((orderSequence % 2) == 1) ? PlaceOrderRequest.Side.BID : PlaceOrderRequest.Side.ASK)
                .setPrice("1.0")
                .setQuantity("1.0")
                .build();

        // 记录发送
        detector.recordSentRequest();

        // 发送订单并处理响应
        service.placeOrder(request, new DetectionStreamObserver());
    }

    /**
     * 集成检测器的StreamObserver
     */
    private class DetectionStreamObserver implements StreamObserver<PlaceOrderResponse> {

        @Override
        public void onNext(PlaceOrderResponse response) {
            // 记录最终响应（Response层处理完成）
            boolean isSuccess = response.getCode() == 1; // OrderCreated
            detector.recordResponseProcessed(isSuccess);
        }

        @Override
        public void onError(Throwable t) {
            // 记录错误响应
            detector.recordResponseProcessed(false);
            System.err.println("❌ 订单处理错误: " + t.getMessage());
        }

        @Override
        public void onCompleted() {
            // gRPC调用完成
        }
    }

    /**
     * 测试背压场景下的完成状态检测
     */
    @Test
    public void testCompletionDetectionUnderBackpressure() throws Exception {
        System.out.println("🔥 测试高负载背压场景下的完成状态检测");

        // 高负载参数：更多订单，更少线程，模拟背压
        int orderCount = 50000;
        int threadCount = 2; // 减少线程数增加背压

        detector.start();

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // 快速发送大量订单
        for (int i = 0; i < orderCount; i++) {
            final int orderId = i + 1;
            executor.submit(() -> sendOrderWithDetection(orderId));
        }

        // 等待系统完成
        SystemCompletionDetector.SystemCompletionResult result = detector.waitForSystemCompletion();

        result.printDetailedSummary();

        // 在背压场景下验证
        System.out.printf("📊 背压测试结果：成功率 %.1f%%, 拒绝率 %.1f%%\n",
                (double)result.finalState().successfulRequests() / result.finalState().sentRequests() * 100,
                (double)result.finalState().rejectedRequests() / result.finalState().sentRequests() * 100);

        executor.shutdown();

        assert result.finalState().sentRequests() == orderCount : "发送订单数不匹配";
        assert result.finalState().responseProcessed() > 0 : "应该有响应处理";

        System.out.println("✅ 背压场景测试完成！");
    }

    /**
     * 测试系统稳定性检测
     */
    @Test
    public void testStabilityDetection() throws Exception {
        System.out.println("🎯 测试系统稳定性检测机制");

        // 使用较严格的稳定性参数
        SystemCompletionDetector strictDetector = new SystemCompletionDetector(service, 30000, 25, 30);
        strictDetector.start();

        // 发送少量订单进行精确检测
        int orderCount = 1000;

        for (int i = 1; i <= orderCount; i++) {
            sendOrderWithStrictDetection(i, strictDetector);
        }

        System.out.println("📤 订单发送完成，开始严格稳定性检测...");

        SystemCompletionDetector.SystemCompletionResult result = strictDetector.waitForSystemCompletion();

        result.printDetailedSummary();

        // 验证稳定性检测的准确性
        assert result.completedNormally() : "严格检测应该正常完成";
        assert result.finalState().responseProcessed() == orderCount : "所有订单都应被处理";

        System.out.println("✅ 稳定性检测测试完成！");
    }

    private void sendOrderWithStrictDetection(int orderSequence, SystemCompletionDetector detector) {
        PlaceOrderRequest request = PlaceOrderRequest.newBuilder()
                .setAccountId(((orderSequence % 2) == 1) ? 1 : 2)
                .setSymbolId(1)
                .setSide(((orderSequence % 2) == 1) ? PlaceOrderRequest.Side.BID : PlaceOrderRequest.Side.ASK)
                .setPrice("1.0")
                .setQuantity("1.0")
                .build();

        detector.recordSentRequest();

        service.placeOrder(request, new StreamObserver<PlaceOrderResponse>() {
            @Override
            public void onNext(PlaceOrderResponse response) {
                boolean isSuccess = response.getCode() == 1;
                detector.recordResponseProcessed(isSuccess);
            }

            @Override
            public void onError(Throwable t) {
                detector.recordResponseProcessed(false);
            }

            @Override
            public void onCompleted() {}
        });
    }
} 