package com.cmex.bolt.domain;

import com.cmex.bolt.util.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

public class TestOrderBook {
    
    private Symbol symbol;
    private OrderBook orderBook;
    
    @BeforeEach
    public void setUp() {
        // 创建测试用的Currency
        Currency base = Currency.builder().id(1).name("BTC").precision(6).build();
        Currency quote = Currency.builder().id(2).name("USDT").precision(4).build();
        
        // 创建测试用的Symbol
        symbol = Symbol.builder()
            .id(1)
            .name("BTCUSDT")
            .base(base)
            .quote(quote)
            .quoteSettlement(true)
            .build();
        symbol.init();
        
        orderBook = symbol.getOrderBook();
    }
    
    @Test
    public void testOrderBookCacheOptimization() {
        // 测试缓存优化是否正确工作
        
        // 1. 创建maker订单（卖单）
        Order maker1 = createLimitSellOrder(1L, 500000000L, 1000000L); // 50000 USDT, 1 BTC
        Order maker2 = createLimitSellOrder(2L, 510000000L, 2000000L); // 51000 USDT, 2 BTC
        
        // 2. 先添加订单到订单簿
        Result<List<Ticket>> result1 = orderBook.match(maker1);
        assertTrue(!result1.isSuccess()); // 没有匹配，应该挂单
        
        Result<List<Ticket>> result2 = orderBook.match(maker2);
        assertTrue(!result2.isSuccess()); // 没有匹配，应该挂单
        
        // 3. 创建taker订单（买单），应该匹配最优价格
        Order taker = createLimitBuyOrder(3L, 520000000L, 1500000L); // 52000 USDT, 1.5 BTC
        
        // 4. 执行匹配，应该先匹配50000价格的订单
        Result<List<Ticket>> matchResult = orderBook.match(taker);
        assertTrue(matchResult.isSuccess());
        
        List<Ticket> tickets = matchResult.value();
        assertFalse(tickets.isEmpty());
        
        // 验证匹配结果
        Ticket firstTicket = tickets.get(0);
        assertEquals(500000000L, firstTicket.getPrice()); // 应该匹配最优价格50000
        assertEquals(1000000L, firstTicket.getQuantity()); // 完全消耗maker1
        
        // 5. 如果还有剩余，应该匹配下一个价格
        if (tickets.size() > 1) {
            Ticket secondTicket = tickets.get(1);
            assertEquals(510000000L, secondTicket.getPrice()); // 下一个价格51000
        }
        
        System.out.println("✅ 缓存优化测试通过！匹配了 " + tickets.size() + " 笔交易");
        for (Ticket ticket : tickets) {
            System.out.printf("   价格: %d, 数量: %d, 成交额: %d%n", 
                ticket.getPrice(), ticket.getQuantity(), ticket.getVolume());
        }
    }
    
    @Test
    public void testCancelOrderCacheUpdate() {
        // 测试撤单时缓存更新是否正确
        
        // 1. 添加订单
        Order order1 = createLimitSellOrder(1L, 500000000L, 1000000L);
        Order order2 = createLimitSellOrder(2L, 510000000L, 1000000L);
        
        orderBook.match(order1);
        orderBook.match(order2);
        
        // 2. 撤销最优价格订单，缓存应该更新
        Result<Order> cancelResult = orderBook.cancel(1L);
        assertTrue(cancelResult.isSuccess());
        
        // 3. 新的买单应该匹配到下一个最优价格
        Order taker = createLimitBuyOrder(3L, 520000000L, 1000000L);
        Result<List<Ticket>> matchResult = orderBook.match(taker);
        
        assertTrue(matchResult.isSuccess());
        Ticket ticket = matchResult.value().get(0);
        assertEquals(510000000L, ticket.getPrice()); // 应该匹配51000而不是50000
        
        System.out.println("✅ 撤单缓存更新测试通过！匹配价格: " + ticket.getPrice());
    }
    
    @Test
    public void testObjectPoolOptimization() {
        // 测试预分配内存优化（对象池）是否正确工作
        
        System.out.println("🔧 开始测试对象池优化...");
        
        // 1. 获取初始对象池状态
        var initialStats = orderBook.getPoolStats();
        System.out.println("初始对象池状态: " + initialStats);
        assertEquals(10, initialStats.getCurrentPoolSize()); // 预热了10个对象
        
        // 2. 创建多个订单进行多次匹配操作
        for (int i = 0; i < 20; i++) {
            // 添加卖单
            Order seller = createLimitSellOrder(i * 2 + 1000L, 500000000L + i * 10000000L, 1000000L);
            orderBook.match(seller);
            
            // 添加买单进行匹配
            Order buyer = createLimitBuyOrder(i * 2 + 1001L, 600000000L, 500000L);
            Result<List<Ticket>> result = orderBook.match(buyer);
            
            if (result.isSuccess() && !result.value().isEmpty()) {
                System.out.printf("第%d次匹配成功，成交%d笔\n", i + 1, result.value().size());
            }
        }
        
        // 3. 检查对象池统计信息
        var finalStats = orderBook.getPoolStats();
        System.out.println("最终对象池状态: " + finalStats);
        
        // 4. 验证对象池工作效果
        assertTrue(finalStats.getBorrowCount() > 0, "对象池应该有借用记录");
        assertTrue(finalStats.getReturnCount() > 0, "应该有对象归还记录");
        assertTrue(finalStats.getHitRate() > 50, "命中率应该超过50%");
        
        // 5. 验证内存使用优化
        long totalRequests = finalStats.getBorrowCount();
        System.out.printf("总请求: %d, 池命中: %d, 命中率: %.1f%%\n", 
            totalRequests, finalStats.getBorrowCount() - finalStats.getCreateCount(), finalStats.getHitRate());
        
        // 预期大部分请求应该从对象池获得对象
        assertTrue(finalStats.getHitRate() > 30, 
            "对象池命中率应该超过30%，实际: " + finalStats.getHitRate() + "%");
        
        System.out.println("✅ 对象池优化测试通过！");
    }
    
    @Test 
    public void testMemoryPreallocationBenefits() {
        // 测试预分配内存的好处
        
        System.out.println("📊 开始压力测试，验证内存预分配效果...");
        
        long startTime = System.nanoTime();
        
        // 执行大量匹配操作
        for (int batch = 0; batch < 10; batch++) {
            for (int i = 0; i < 50; i++) {
                // 创建卖单
                Order seller = createLimitSellOrder(
                    batch * 50 + i + 2000L, 
                    500000000L + i * 5000000L, 
                    1000000L + i * 100000L
                );
                orderBook.match(seller);
                
                // 创建买单匹配
                Order buyer = createLimitBuyOrder(
                    batch * 50 + i + 2500L,
                    600000000L,
                    800000L + i * 50000L
                );
                orderBook.match(buyer);
            }
        }
        
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        
        // 获取最终统计
        var stats = orderBook.getPoolStats();
        
        System.out.printf("压力测试完成:\n");
        System.out.printf("  执行时间: %.2f ms\n", duration / 1_000_000.0);
        System.out.printf("  对象池效果: %s\n", stats);
        System.out.printf("  内存优化指标: 命中率 %.1f%%, 避免分配 %d 次\n", 
            stats.getHitRate(), stats.getBorrowCount() - stats.getCreateCount());
        
        // 验证性能指标
        assertTrue(stats.getBorrowCount() > 100, "应该有大量的池借用");
        assertTrue(stats.getHitRate() > 20, "命中率应该合理");
        
        System.out.println("✅ 内存预分配压力测试通过！");
        
        // 打印详细性能报告
        System.out.println("\n" + stats.getDetailedReport());
    }
    
    private Order createLimitBuyOrder(long id, long price, long quantity) {
        return Order.builder()
            .id(id)
            .symbolId(1)
            .accountId(100)
            .type(Order.Type.LIMIT)
            .side(Order.Side.BID)
            .specification(Order.Specification.limitByQuantity(new BigDecimal(price), new BigDecimal(quantity)))
            .fee(Order.Fee.builder().taker(30).maker(20).build())
            .frozen(BigDecimal.ZERO)
            .build();
    }
    
    private Order createLimitSellOrder(long id, long price, long quantity) {
        return Order.builder()
            .id(id)
            .symbolId(1)
            .accountId(200)
            .type(Order.Type.LIMIT)
            .side(Order.Side.ASK)
            .specification(Order.Specification.limitByQuantity(new BigDecimal(price), new BigDecimal(quantity)))
            .fee(Order.Fee.builder().taker(30).maker(20).build())
            .frozen(BigDecimal.ZERO)
            .build();
    }
}
