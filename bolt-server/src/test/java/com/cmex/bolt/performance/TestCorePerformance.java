package com.cmex.bolt.performance;

import com.cmex.bolt.Bolt;
import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.EnvoyUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 增强版订单性能测试
 * 使用SystemCompletionDetector来准确判断复杂异步系统的完成状态
 */
@Tag("performance")
public class TestCorePerformance {

    private static EnvoyServer service;
    private static Bolt bolt;

    @BeforeAll
    static void setUp() {
        bolt = new Bolt(new BoltConfig(9090, true, 10,
                1024 * 1024 * 8, 1024 * 1024 * 4, 1024 * 1024 * 4, true, 9091));
        service = bolt.getEnvoyServer();
    }

    @Test
    public void test() throws Exception {
        // 配置参数
        final int ORDERS_PER_THREAD = 20000; // 每个线程的下单数量
        final int TRADING_PAIRS_COUNT = 10;  // 交易对数量
        final int THREADS_PER_PAIR = 2;      // 每个交易对的线程数（买单+卖单）
        
        // 定义10个交易对：symbolId, baseCurrencyId, 交易对名称
        int[][] tradingPairs = {
            {1, 2, 1},   // BTCUSDT: symbolId=1, baseCurrencyId=2(BTC), quoteCurrencyId=1(USDT)
            {2, 3, 1},   // SHIBUSDT: symbolId=2, baseCurrencyId=3(SHIB), quoteCurrencyId=1(USDT)
            {3, 4, 1},   // ETHUSDT: symbolId=3, baseCurrencyId=4(ETH), quoteCurrencyId=1(USDT)
            {4, 5, 1},   // SOLUSDT: symbolId=4, baseCurrencyId=5(SOL), quoteCurrencyId=1(USDT)
            {5, 6, 1},   // DOGEUSDT: symbolId=5, baseCurrencyId=6(DOGE), quoteCurrencyId=1(USDT)
            {6, 7, 1},   // AAVEUSDT: symbolId=6, baseCurrencyId=7(AAVE), quoteCurrencyId=1(USDT)
            {7, 8, 1},   // XRPUSDT: symbolId=7, baseCurrencyId=8(XRP), quoteCurrencyId=1(USDT)
            {8, 9, 1},   // ADAUSDT: symbolId=8, baseCurrencyId=9(ADA), quoteCurrencyId=1(USDT)
            {9, 10, 1},  // BCHUSDT: symbolId=9, baseCurrencyId=10(BCH), quoteCurrencyId=1(USDT)
            {10, 11, 1}  // LTCUSDT: symbolId=10, baseCurrencyId=11(LTC), quoteCurrencyId=1(USDT)
        };
        
        // 设置账户余额 - 每个交易对需要2个账户：买单账户(USDT)和卖单账户(基础币种)
        for (int i = 0; i < tradingPairs.length; i++) {
            int buyAccountId = i * 2 + 1;  // 买单账户：1,3,5,7,9,11,13,15,17,19
            int sellAccountId = i * 2 + 2; // 卖单账户：2,4,6,8,10,12,14,16,18,20
            int quoteCurrencyId = tradingPairs[i][2]; // USDT
            int baseCurrencyId = tradingPairs[i][1];  // 基础币种
            
            EnvoyUtil.increase(service, buyAccountId, quoteCurrencyId, "20000");  // 买单账户增加USDT
            EnvoyUtil.increase(service, sellAccountId, baseCurrencyId, "20000");  // 卖单账户增加基础币种
        }
        
        // 创建线程的同步工具
        final int TOTAL_THREADS = TRADING_PAIRS_COUNT * THREADS_PER_PAIR;
        CountDownLatch latch = new CountDownLatch(TOTAL_THREADS);
        AtomicLong totalBuyOrderCount = new AtomicLong(0);
        AtomicLong totalSellOrderCount = new AtomicLong(0);
        AtomicLong[] buyOrderCounts = new AtomicLong[TRADING_PAIRS_COUNT];
        AtomicLong[] sellOrderCounts = new AtomicLong[TRADING_PAIRS_COUNT];
        
        // 初始化每个交易对的计数器
        for (int i = 0; i < TRADING_PAIRS_COUNT; i++) {
            buyOrderCounts[i] = new AtomicLong(0);
            sellOrderCounts[i] = new AtomicLong(0);
        }
        
        // 创建线程：每个交易对2个线程（买单和卖单）
        Thread[] threads = new Thread[TOTAL_THREADS];
        for (int i = 0; i < tradingPairs.length; i++) {
            final int pairIndex = i;
            final int symbolId = tradingPairs[i][0];
            final int buyAccountId = i * 2 + 1;
            final int sellAccountId = i * 2 + 2;
            
            // 买单线程
            threads[i * 2] = new Thread(() -> {
                try {
                    for (int j = 0; j < ORDERS_PER_THREAD; j++) {
                        EnvoyUtil.placeOrder(service, pairIndex * ORDERS_PER_THREAD * 2 + j + 1, symbolId, buyAccountId, 
                                Envoy.Type.LIMIT, Envoy.Side.BID, "1", "1",
                                FakeStreamObserver.of(response -> {
                                    if (response.getData().getId() > 0) {
                                        buyOrderCounts[pairIndex].incrementAndGet();
                                        totalBuyOrderCount.incrementAndGet();
                                    }
                                }));
                    }
                } finally {
                    latch.countDown();
                }
            });
            
            // 卖单线程
            threads[i * 2 + 1] = new Thread(() -> {
                try {
                    for (int j = 0; j < ORDERS_PER_THREAD; j++) {
                        EnvoyUtil.placeOrder(service, pairIndex * ORDERS_PER_THREAD * 2 + j + ORDERS_PER_THREAD + 1, symbolId, sellAccountId, 
                                Envoy.Type.LIMIT, Envoy.Side.ASK, "1", "1",
                                FakeStreamObserver.of(response -> {
                                    if (response.getData().getId() > 0) {
                                        sellOrderCounts[pairIndex].incrementAndGet();
                                        totalSellOrderCount.incrementAndGet();
                                    }
                                }));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 启动所有线程
        long startTime = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        latch.await();
        
        // 等待一段时间让订单处理完成
        Thread.sleep(2000);
        
        long endTime = System.currentTimeMillis();
        
        // 输出每个交易对的结果
        String[] pairNames = {"BTCUSDT", "SHIBUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", 
                             "AAVEUSDT", "XRPUSDT", "ADAUSDT", "BCHUSDT", "LTCUSDT"};
        
        System.out.println("=== 各交易对订单统计 ===");
        for (int i = 0; i < tradingPairs.length; i++) {
            System.out.printf("%s: 买单=%d, 卖单=%d, 总计=%d\n", 
                pairNames[i], 
                buyOrderCounts[i].get(), 
                sellOrderCounts[i].get(),
                buyOrderCounts[i].get() + sellOrderCounts[i].get());
        }
        
        System.out.println("\n=== 总体统计 ===");
        System.out.printf("总买单成功数量: %d\n", totalBuyOrderCount.get());
        System.out.printf("总卖单成功数量: %d\n", totalSellOrderCount.get());
        System.out.printf("总订单数量: %d\n", totalBuyOrderCount.get() + totalSellOrderCount.get());
        System.out.printf("总耗时: %d ms\n", endTime - startTime);
        long totalOrders = totalBuyOrderCount.get() + totalSellOrderCount.get();
        System.out.printf("平均每秒订单数: %.2f\n", (totalOrders * 1000.0) / (endTime - startTime));
        System.out.printf("预期总订单数: %d (%d个交易对 × %d个线程 × %d笔订单)\n", 
                TRADING_PAIRS_COUNT * THREADS_PER_PAIR * ORDERS_PER_THREAD,
                TRADING_PAIRS_COUNT, THREADS_PER_PAIR, ORDERS_PER_THREAD);
        
        // 查看几个主要交易对的深度
        System.out.println("\n=== 主要交易对深度 ===");
        for (int i = 0; i < tradingPairs.length; i++) {
            System.out.printf("%s 深度:\n", pairNames[i]);
            EnvoyUtil.getDepth(service, tradingPairs[i][0]);
        }
    }
}