package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.cmex.bolt.spot.util.BackpressureManager;
import com.cmex.bolt.spot.monitor.RingBufferMonitor;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;
import static com.cmex.bolt.spot.util.SpotServiceUtil.*;

/**
 * 测试BackpressureManager的效果
 */
public class TestMatchWithBackpressure {
    private static SpotServiceImpl service = new SpotServiceImpl();
    private static int TIMES = 5000; // 故意设置高值来触发背压

    @BeforeAll
    public static void init() {
        increase(service, 1, 1, String.valueOf(TIMES));
        increase(service, 2, 2, String.valueOf(TIMES));
        increase(service, 3, 1, String.valueOf(TIMES));
        increase(service, 4, 3, String.valueOf(TIMES));
        
        System.out.println("=== 初始化完成，开始背压测试 ===");
        service.reportMonitorStats(); // 初始状态报告
    }

    @AfterAll
    public static void cleanup() {
        service.shutdown(); // 关闭监控器
    }

    @Test
    public void testOrderWithBackpressure() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(4);
        
        // 统计计数器
        AtomicLong successCount = new AtomicLong();
        AtomicLong rejectedCount = new AtomicLong();
        AtomicLong errorCount = new AtomicLong();
        
        System.out.println("开始发送 " + (TIMES * 4) + " 个订单...");
        
        // 第一个线程 - BID订单
        executor.submit(() -> {
            for (int i = 1; i <= TIMES; i++) {
                try {
                    placeOrderWithCallback(1, 1, PlaceOrderRequest.Type.LIMIT, 
                        PlaceOrderRequest.Side.BID, "1", "1", successCount, rejectedCount);
                    
                    // 每1000个订单输出一次状态
                    if (i % 1000 == 0) {
                        System.out.printf("Thread-1: 已发送 %d 个BID订单%n", i);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }
            System.out.println("BID订单发送完成");
            latch.countDown();
        });
        
        // 第二个线程 - ASK订单  
        executor.submit(() -> {
            for (int i = 1; i <= TIMES; i++) {
                try {
                    placeOrderWithCallback(1, 2, PlaceOrderRequest.Type.LIMIT, 
                        PlaceOrderRequest.Side.ASK, "1", "1", successCount, rejectedCount);
                    
                    if (i % 1000 == 0) {
                        System.out.printf("Thread-2: 已发送 %d 个ASK订单%n", i);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }
            System.out.println("ASK订单发送完成");
            latch.countDown();
        });
        
        // 第三个线程 - Symbol 2 BID
        executor.submit(() -> {
            for (int i = 1; i <= TIMES; i++) {
                try {
                    placeOrderWithCallback(2, 3, PlaceOrderRequest.Type.LIMIT, 
                        PlaceOrderRequest.Side.BID, "1", "1", successCount, rejectedCount);
                    
                    if (i % 1000 == 0) {
                        System.out.printf("Thread-3: 已发送 %d 个BID订单(Symbol2)%n", i);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }
            System.out.println("Symbol2 BID订单发送完成");
            latch.countDown();
        });
        
        // 第四个线程 - Symbol 2 ASK
        executor.submit(() -> {
            for (int i = 1; i <= TIMES; i++) {
                try {
                    placeOrderWithCallback(2, 4, PlaceOrderRequest.Type.LIMIT, 
                        PlaceOrderRequest.Side.ASK, "1", "1", successCount, rejectedCount);
                    
                    if (i % 1000 == 0) {
                        System.out.printf("Thread-4: 已发送 %d 个ASK订单(Symbol2)%n", i);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
            }
            System.out.println("Symbol2 ASK订单发送完成");
            latch.countDown();
        });
        
        // 等待所有线程完成发送
        latch.await();
        System.out.println("所有订单发送完成，用时: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        
        // 输出统计信息
        System.out.println("=== 发送阶段统计 ===");
        System.out.println("成功: " + successCount.get());
        System.out.println("拒绝: " + rejectedCount.get());
        System.out.println("错误: " + errorCount.get());
        System.out.println("总计: " + (successCount.get() + rejectedCount.get() + errorCount.get()));
        
        // 输出背压统计
        System.out.println("\n=== 背压统计 ===");
        service.reportMonitorStats();
        
        // 获取详细的背压统计
        BackpressureManager.BackpressureStats accountStats = service.getAccountBackpressureStats();
        BackpressureManager.BackpressureStats matchStats = service.getMatchBackpressureStats();
        BackpressureManager.BackpressureStats responseStats = service.getResponseBackpressureStats();
        
        System.out.println("\n=== 详细统计 ===");
        System.out.printf("Account RingBuffer: 拒绝率 %.2f%%, 最大使用率 %.2f%%%n",
            accountStats.getRejectionRate() * 100, accountStats.maxUsageRate() * 100);
        System.out.printf("Match RingBuffer: 拒绝率 %.2f%%, 最大使用率 %.2f%%%n",
            matchStats.getRejectionRate() * 100, matchStats.maxUsageRate() * 100);
        System.out.printf("Response RingBuffer: 拒绝率 %.2f%%, 最大使用率 %.2f%%%n",
            responseStats.getRejectionRate() * 100, responseStats.maxUsageRate() * 100);
        
        // 等待撮合完成的简化检查
        System.out.println("\n等待撮合完成...");
        TimeUnit.SECONDS.sleep(5); // 给一些时间让撮合完成
        
        // 最终报告
        System.out.println("\n=== 最终报告 ===");
        service.reportMonitorStats();
        
        RingBufferMonitor.SummaryStats summary = service.getMonitorSummary();
        System.out.printf("汇总: 总请求=%d, 总拒绝=%d, 拒绝率=%.2f%%, 最大使用率=%.2f%%, 临界RingBuffer数=%d%n",
            summary.totalRequests(), summary.totalRejected(), summary.getRejectionRate() * 100,
            summary.maxUsageRate() * 100, summary.criticalRingBuffers());
        
        executor.shutdown();
        
        // 验证背压机制有效性
        if (rejectedCount.get() > 0) {
            System.out.println("✅ 背压机制生效！成功拒绝了 " + rejectedCount.get() + " 个请求");
        } else {
            System.out.println("ℹ️  所有请求都被接受，可能需要增加负载或减少RingBuffer容量来测试背压");
        }
    }
    
    /**
     * 带回调的下单方法，用于统计成功和拒绝数
     */
    private static void placeOrderWithCallback(int symbolId, int accountId, 
                                             PlaceOrderRequest.Type type, PlaceOrderRequest.Side side,
                                             String price, String quantity,
                                             AtomicLong successCount, AtomicLong rejectedCount) {
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(symbolId)
                .setAccountId(accountId)
                .setType(type)
                .setSide(side)
                .setPrice(price)
                .setQuantity(quantity)
                .build(), new FakeStreamObserver<>(response -> {
            if (response.getCode() == 1) {
                successCount.incrementAndGet();
            } else if (response.getCode() == RejectionReason.SYSTEM_BUSY.getCode()) {
                rejectedCount.incrementAndGet();
//                 可以选择打印拒绝消息
                 System.out.println("订单被拒绝: " + response.getMessage());
            } else {
                // 其他错误
                System.err.println("订单错误: " + response.getMessage());
            }
        }));
    }

    /**
     * 压力测试 - 更高强度的负载
     */
    @Test 
    public void testHighLoadStress() throws InterruptedException {
        System.out.println("=== 开始高强度压力测试 ===");
        
        // 重置统计
        service.resetAllStats();
        
        int highLoadTimes = 100000; // 更高的负载
        ExecutorService executor = Executors.newFixedThreadPool(16); // 更多线程
        AtomicLong totalSuccess = new AtomicLong();
        AtomicLong totalRejected = new AtomicLong();
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        
        // 启动多个线程同时发送请求
        for (int t = 0; t < 8; t++) {
            final int threadId = t;
            executor.submit(() -> {
                for (int i = 0; i < highLoadTimes / 8; i++) {
                    try {
                        placeOrderWithCallback(1, 1 + (threadId % 4), PlaceOrderRequest.Type.LIMIT,
                            threadId % 2 == 0 ? PlaceOrderRequest.Side.BID : PlaceOrderRequest.Side.ASK,
                            "1", "1", totalSuccess, totalRejected);
                        
                        // 短暂休息避免过度竞争
                        if (i % 100 == 0) {
                            Thread.yield();
                        }
                    } catch (Exception e) {
                        System.err.println("线程 " + threadId + " 错误: " + e.getMessage());
                    }
                }
                System.out.println("线程 " + threadId + " 完成");
            });
        }
        
        // 等待完成
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        
        System.out.println("压力测试完成，用时: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
        System.out.println("成功: " + totalSuccess.get() + ", 拒绝: " + totalRejected.get());
        
        // 最终统计
        service.reportMonitorStats();
    }
} 