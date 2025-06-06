package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.api.PlaceOrder;
import com.cmex.bolt.spot.domain.Order;
import com.cmex.bolt.spot.domain.Symbol;
import com.cmex.bolt.spot.domain.Currency;
import com.cmex.bolt.spot.util.OrderPool;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCreateOrder {

    private final int times = 1_000_000;

    @Test
    public void testCreateOrder() {
        // 创建测试用的Symbol
        Symbol testSymbol = Symbol.builder()
                .id(1)
                .name("BTCUSDT")
                .base(Currency.builder().id(1).name("BTC").precision(6).build())
                .quote(Currency.builder().id(2).name("USDT").precision(4).build())
                .quoteSettlement(true)
                .build();
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        List<Order> orders = new ArrayList<Order>(times);
        
        // 测试新的javolution Order对象创建性能
        for (int i = 0; i < times; i++) {
            Order order = new Order();
            order.setByteBuffer(ByteBuffer.allocateDirect(order.size()), 0);
            order.init(testSymbol, i, 1, Order.OrderType.LIMIT, Order.OrderSide.BID, 
                      100000, 1000, 100000000, 100000000, 100, 50);
            orders.add(order);
        }
        System.out.println("javolution Order creation elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        
        // 重置计时器
        stopwatch.reset().start();
        
        // 测试对象池性能
        OrderPool orderPool = new OrderPool(1000); // 创建对象池
        List<Order> pooledOrders = new ArrayList<Order>(times);
        for (int i = 0; i < times; i++) {
            Order order = orderPool.acquireAndInit(testSymbol, i, 1, Order.OrderType.LIMIT, 
                                                  Order.OrderSide.BID, 100000, 1000, 100000000, 
                                                  100000000, 100, 50);
            pooledOrders.add(order);
            // 在实际使用中，这里会释放对象回池
            orderPool.release(order);
        }
        System.out.println("Object pool elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        
        // 重置计时器测试PlaceOrder
        stopwatch.reset().start();
        List<PlaceOrder> placeOrders = new ArrayList<PlaceOrder>(times);
        for (int i = 0; i < times; i++) {
            PlaceOrder order = new PlaceOrder();
            placeOrders.add(order);
        }
        System.out.println("PlaceOrder creation elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
