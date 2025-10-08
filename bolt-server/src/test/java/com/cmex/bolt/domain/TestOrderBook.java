package com.cmex.bolt.domain;

import com.cmex.bolt.util.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
        assertEquals(new BigDecimal("500000000"), firstTicket.getPrice()); // 应该匹配最优价格50000
        assertEquals(new BigDecimal("1000000"), firstTicket.getQuantity()); // 完全消耗maker1
        
        // 5. 如果还有剩余，应该匹配下一个价格
        if (tickets.size() > 1) {
            Ticket secondTicket = tickets.get(1);
            assertEquals(new BigDecimal("510000000"), secondTicket.getPrice()); // 下一个价格51000
        }
        
        System.out.println("✅ 缓存优化测试通过！匹配了 " + tickets.size() + " 笔交易");
        for (Ticket ticket : tickets) {
            System.out.printf("   价格: %s, 数量: %s, 成交额: %s%n", 
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
        assertEquals(new BigDecimal("510000000"), ticket.getPrice()); // 应该匹配51000而不是50000
        
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
    
    @Test
    public void testOrderBookJsonSerializationAndDeserialization() {
        // 测试OrderBook的JSON序列化和反序列化功能
        System.out.println("🔄 开始测试OrderBook JSON序列化和反序列化...");
        
        ObjectMapper objectMapper = new ObjectMapper();
        
        // 1. 准备测试数据 - 创建包含多个订单的OrderBook
        setupTestOrderBookData();
        
        // 2. 序列化OrderBook到JSON
        String jsonString = null;
        try {
            jsonString = objectMapper.writeValueAsString(orderBook);
            System.out.println("✅ OrderBook序列化成功");
            System.out.println("JSON长度: " + jsonString.length() + " 字符");
            
            // 打印部分JSON内容用于调试
            if (jsonString.length() > 200) {
                System.out.println("JSON预览: " + jsonString.substring(0, 200) + "...");
            } else {
                System.out.println("完整JSON: " + jsonString);
            }
        } catch (Exception e) {
            fail("OrderBook序列化失败: " + e.getMessage());
        }
        
        // 3. 验证JSON结构
        assertNotNull(jsonString, "序列化结果不应为空");
        assertTrue(jsonString.contains("bids"), "JSON应包含bids字段");
        assertTrue(jsonString.contains("asks"), "JSON应包含asks字段");
        
        // 4. 测试JSON结构的完整性
        verifyJsonStructure(jsonString);
        
        // 5. 测试序列化后与反序列化后的对象比较
        testSerializationDeserializationComparison(objectMapper, jsonString);
        
        // 6. 测试SymbolRepository的getAllData()方法模拟
        testSymbolRepositoryDataSerialization(objectMapper);
        
        // 7. 测试深度数据结构序列化
        testDepthDataSerialization(objectMapper);
        
        System.out.println("✅ OrderBook JSON序列化和反序列化测试全部通过！");
    }
    
    /**
     * 设置测试用的OrderBook数据
     */
    private void setupTestOrderBookData() {
        System.out.println("📝 设置测试OrderBook数据...");
        
        // 创建多个不同价格的订单
        Order[] orders = {
            // 买单 (BID)
            createLimitBuyOrder(1001L, 480000000L, 2000000L),  // 48000 USDT, 2 BTC
            createLimitBuyOrder(1002L, 490000000L, 1500000L),  // 49000 USDT, 1.5 BTC
            createLimitBuyOrder(1003L, 495000000L, 1000000L),  // 49500 USDT, 1 BTC
            
            // 卖单 (ASK)
            createLimitSellOrder(2001L, 500000000L, 1000000L), // 50000 USDT, 1 BTC
            createLimitSellOrder(2002L, 510000000L, 2000000L), // 51000 USDT, 2 BTC
            createLimitSellOrder(2003L, 520000000L, 1500000L), // 52000 USDT, 1.5 BTC
        };
        
        // 将订单添加到OrderBook
        for (Order order : orders) {
            orderBook.match(order);
        }
        
        System.out.printf("✅ 已添加 %d 个订单到OrderBook\n", orders.length);
        System.out.printf("   买单层级数: %d\n", orderBook.getBids().size());
        System.out.printf("   卖单层级数: %d\n", orderBook.getAsks().size());
    }
    
    /**
     * 验证JSON结构的完整性
     */
    private void verifyJsonStructure(String jsonString) {
        System.out.println("🔍 验证JSON结构完整性...");
        
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(jsonString);
            
            // 验证基本结构
            assertTrue(jsonNode.has("bids"), "JSON应包含bids字段");
            assertTrue(jsonNode.has("asks"), "JSON应包含asks字段");
            
            JsonNode bidsNode = jsonNode.get("bids");
            JsonNode asksNode = jsonNode.get("asks");
            
            // 验证bids结构
            assertTrue(bidsNode.isObject(), "bids应该是对象");
            assertEquals(orderBook.getBids().size(), bidsNode.size(), 
                "bids层级数量应该与原始数据一致");
            
            // 验证asks结构
            assertTrue(asksNode.isObject(), "asks应该是对象");
            assertEquals(orderBook.getAsks().size(), asksNode.size(), 
                "asks层级数量应该与原始数据一致");
            
            // 验证价格层级结构
            verifyPriceLevelStructure(bidsNode, "bids");
            verifyPriceLevelStructure(asksNode, "asks");
            
            System.out.println("✅ JSON结构验证通过");
            
        } catch (Exception e) {
            fail("JSON结构验证失败: " + e.getMessage());
        }
    }
    
    /**
     * 验证价格层级结构
     */
    private void verifyPriceLevelStructure(JsonNode priceLevelsNode, String side) {
        System.out.println("🔍 验证" + side + "价格层级结构...");
        
        priceLevelsNode.fields().forEachRemaining(entry -> {
            String priceStr = entry.getKey();
            JsonNode priceNode = entry.getValue();
            
            // 验证价格节点结构
            assertTrue(priceNode.has("price"), "价格节点应包含price字段");
            assertTrue(priceNode.has("quantity"), "价格节点应包含quantity字段");
            
            // 验证数据类型
            assertTrue(priceNode.get("price").isNumber(), "price应该是数字");
            assertTrue(priceNode.get("quantity").isNumber(), "quantity应该是数字");
            
            // 验证价格值
            BigDecimal price = new BigDecimal(priceStr);
            BigDecimal nodePrice = priceNode.get("price").decimalValue();
            assertEquals(price, nodePrice, "价格值应该一致");
            
            System.out.printf("  ✅ 价格层级 %s: price=%s, quantity=%s\n", 
                side, priceStr, priceNode.get("quantity").asText());
        });
    }
    
    /**
     * 测试序列化后与反序列化后的对象比较
     */
    private void testSerializationDeserializationComparison(ObjectMapper objectMapper, String jsonString) {
        System.out.println("🔄 测试序列化后与反序列化后的对象比较...");
        
        try {
            // 1. 将JSON反序列化为Map结构进行比较
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedData = objectMapper.readValue(jsonString, Map.class);
            
            // 2. 验证反序列化后的基本结构
            assertNotNull(deserializedData, "反序列化结果不应为空");
            assertTrue(deserializedData.containsKey("bids"), "应包含bids字段");
            assertTrue(deserializedData.containsKey("asks"), "应包含asks字段");
            
            // 3. 比较bids数据
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedBids = (Map<String, Object>) deserializedData.get("bids");
            comparePriceLevels(orderBook.getBids(), deserializedBids, "bids");
            
            // 4. 比较asks数据
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedAsks = (Map<String, Object>) deserializedData.get("asks");
            comparePriceLevels(orderBook.getAsks(), deserializedAsks, "asks");
            
            // 5. 测试深度数据的序列化反序列化比较
            testDepthDataSerializationDeserializationComparison(objectMapper);
            
            System.out.println("✅ 序列化反序列化对象比较测试通过");
            
        } catch (Exception e) {
            fail("序列化反序列化对象比较失败: " + e.getMessage());
        }
    }
    
    /**
     * 比较价格层级数据
     */
    private void comparePriceLevels(TreeMap<BigDecimal, PriceNode> originalLevels, 
                                   Map<String, Object> deserializedLevels, String side) {
        System.out.println("🔍 比较" + side + "价格层级数据...");
        
        // 验证层级数量
        assertEquals(originalLevels.size(), deserializedLevels.size(), 
            side + "层级数量应该一致");
        
        // 比较每个价格层级
        for (Map.Entry<BigDecimal, PriceNode> originalEntry : originalLevels.entrySet()) {
            BigDecimal price = originalEntry.getKey();
            PriceNode originalNode = originalEntry.getValue();
            String priceKey = price.toString();
            
            // 验证反序列化数据包含该价格
            assertTrue(deserializedLevels.containsKey(priceKey), 
                "反序列化数据应包含价格: " + priceKey);
            
            // 获取反序列化的价格节点数据
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedNode = (Map<String, Object>) deserializedLevels.get(priceKey);
            
            // 比较价格
            BigDecimal deserializedPrice = new BigDecimal(deserializedNode.get("price").toString());
            assertEquals(originalNode.getPrice(), deserializedPrice, 
                "价格应该一致: " + priceKey);
            
            // 比较数量
            BigDecimal deserializedQuantity = new BigDecimal(deserializedNode.get("quantity").toString());
            assertEquals(originalNode.getQuantity(), deserializedQuantity, 
                "数量应该一致: " + priceKey);
            
            System.out.printf("  ✅ %s价格层级 %s: 价格一致=%s, 数量一致=%s\n", 
                side, priceKey, deserializedPrice, deserializedQuantity);
        }
    }
    
    /**
     * 测试深度数据的序列化反序列化比较
     */
    private void testDepthDataSerializationDeserializationComparison(ObjectMapper objectMapper) {
        System.out.println("📊 测试深度数据的序列化反序列化比较...");
        
        try {
            // 1. 获取原始深度数据
            var originalDepth = orderBook.getDepth();
            assertNotNull(originalDepth, "原始深度数据不应为空");
            
            // 2. 序列化深度数据
            String depthJson = objectMapper.writeValueAsString(originalDepth);
            
            // 3. 反序列化深度数据
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedDepth = objectMapper.readValue(depthJson, Map.class);
            
            // 4. 比较深度数据
            assertEquals(originalDepth.getSymbol(), deserializedDepth.get("symbol"), 
                "symbol名称应该一致");
            
            // 比较bids
            @SuppressWarnings("unchecked")
            Map<String, String> originalBids = originalDepth.getBids();
            @SuppressWarnings("unchecked")
            Map<String, String> deserializedBids = (Map<String, String>) deserializedDepth.get("bids");
            
            assertEquals(originalBids.size(), deserializedBids.size(), 
                "深度数据bids层级数量应该一致");
            
            for (Map.Entry<String, String> entry : originalBids.entrySet()) {
                String price = entry.getKey();
                String quantity = entry.getValue();
                
                assertTrue(deserializedBids.containsKey(price), 
                    "反序列化深度数据应包含价格: " + price);
                assertEquals(quantity, deserializedBids.get(price), 
                    "深度数据数量应该一致: " + price);
            }
            
            // 比较asks
            @SuppressWarnings("unchecked")
            Map<String, String> originalAsks = originalDepth.getAsks();
            @SuppressWarnings("unchecked")
            Map<String, String> deserializedAsks = (Map<String, String>) deserializedDepth.get("asks");
            
            assertEquals(originalAsks.size(), deserializedAsks.size(), 
                "深度数据asks层级数量应该一致");
            
            for (Map.Entry<String, String> entry : originalAsks.entrySet()) {
                String price = entry.getKey();
                String quantity = entry.getValue();
                
                assertTrue(deserializedAsks.containsKey(price), 
                    "反序列化深度数据应包含价格: " + price);
                assertEquals(quantity, deserializedAsks.get(price), 
                    "深度数据数量应该一致: " + price);
            }
            
            System.out.println("✅ 深度数据序列化反序列化比较测试通过");
            
        } catch (Exception e) {
            fail("深度数据序列化反序列化比较失败: " + e.getMessage());
        }
    }
    
    /**
     * 测试SymbolRepository的getAllData()方法模拟
     */
    private void testSymbolRepositoryDataSerialization(ObjectMapper objectMapper) {
        System.out.println("🏪 测试SymbolRepository数据序列化模拟...");
        
        // 模拟SymbolRepository.getAllData()返回的数据结构
        Map<String, Object> symbolData = new java.util.HashMap<>();
        symbolData.put("symbolId", symbol.getId());
        symbolData.put("symbolName", symbol.getName());
        symbolData.put("orderBook", orderBook);
        
        // 模拟MatchingSnapshotHandler中的数据结构
        Map<String, Object> matchingData = new java.util.HashMap<>();
        matchingData.put("timestamp", System.currentTimeMillis());
        matchingData.put("symbols", symbolData);
        
        try {
            // 序列化完整的数据结构
            String fullJsonString = objectMapper.writeValueAsString(matchingData);
            System.out.println("✅ SymbolRepository数据结构序列化成功");
            System.out.println("完整数据结构JSON长度: " + fullJsonString.length() + " 字符");
            
            // 验证JSON包含必要字段
            assertTrue(fullJsonString.contains("timestamp"), "应包含timestamp字段");
            assertTrue(fullJsonString.contains("symbols"), "应包含symbols字段");
            assertTrue(fullJsonString.contains("orderBook"), "应包含orderBook字段");
            assertTrue(fullJsonString.contains("bids"), "应包含bids字段");
            assertTrue(fullJsonString.contains("asks"), "应包含asks字段");
            
            // 反序列化验证
            @SuppressWarnings("unchecked")
            Map<String, Object> deserializedData = objectMapper.readValue(fullJsonString, Map.class);
            assertNotNull(deserializedData, "反序列化结果不应为空");
            assertTrue(deserializedData.containsKey("timestamp"), "应包含timestamp");
            assertTrue(deserializedData.containsKey("symbols"), "应包含symbols");
            
            System.out.println("✅ SymbolRepository数据结构序列化测试通过");
            
        } catch (Exception e) {
            fail("SymbolRepository数据结构序列化失败: " + e.getMessage());
        }
    }
    
    /**
     * 测试深度数据结构序列化
     */
    private void testDepthDataSerialization(ObjectMapper objectMapper) {
        System.out.println("📊 测试深度数据结构序列化...");
        
        try {
            // 获取OrderBook的深度数据
            var depthDto = orderBook.getDepth();
            assertNotNull(depthDto, "深度数据不应为空");
            
            // 序列化深度数据
            String depthJson = objectMapper.writeValueAsString(depthDto);
            System.out.println("✅ 深度数据序列化成功");
            System.out.println("深度数据JSON长度: " + depthJson.length() + " 字符");
            
            // 验证深度数据JSON结构
            JsonNode depthNode = objectMapper.readTree(depthJson);
            assertTrue(depthNode.has("symbol"), "应包含symbol字段");
            assertTrue(depthNode.has("bids"), "应包含bids字段");
            assertTrue(depthNode.has("asks"), "应包含asks字段");
            
            // 验证symbol字段
            assertEquals(symbol.getName(), depthNode.get("symbol").asText(), 
                "symbol名称应该一致");
            
            // 验证bids和asks结构
            JsonNode bidsNode = depthNode.get("bids");
            JsonNode asksNode = depthNode.get("asks");
            
            assertTrue(bidsNode.isObject(), "bids应该是对象");
            assertTrue(asksNode.isObject(), "asks应该是对象");
            
            // 验证价格格式（应该是字符串格式）
            bidsNode.fields().forEachRemaining(entry -> {
                String priceStr = entry.getKey();
                String quantityStr = entry.getValue().asText();
                
                // 验证价格和数量都是有效的数字字符串
                assertDoesNotThrow(() -> new BigDecimal(priceStr), 
                    "价格应该是有效的数字字符串: " + priceStr);
                assertDoesNotThrow(() -> new BigDecimal(quantityStr), 
                    "数量应该是有效的数字字符串: " + quantityStr);
                
                System.out.printf("  ✅ 买单: price=%s, quantity=%s\n", priceStr, quantityStr);
            });
            
            asksNode.fields().forEachRemaining(entry -> {
                String priceStr = entry.getKey();
                String quantityStr = entry.getValue().asText();
                
                // 验证价格和数量都是有效的数字字符串
                assertDoesNotThrow(() -> new BigDecimal(priceStr), 
                    "价格应该是有效的数字字符串: " + priceStr);
                assertDoesNotThrow(() -> new BigDecimal(quantityStr), 
                    "数量应该是有效的数字字符串: " + quantityStr);
                
                System.out.printf("  ✅ 卖单: price=%s, quantity=%s\n", priceStr, quantityStr);
            });
            
            System.out.println("✅ 深度数据结构序列化测试通过");
            
        } catch (Exception e) {
            fail("深度数据结构序列化失败: " + e.getMessage());
        }
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
