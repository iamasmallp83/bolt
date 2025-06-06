package com.cmex.bolt.spot.domain;

import org.junit.jupiter.api.Test;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        Symbol btcusdt = Symbol.builder()
                .id(1)
                .name("BTC/USDT")
                .base(Currency.builder().id(1).name("BTC").precision(6).build())
                .quote(Currency.builder().id(2).name("USDT").precision(4).build())
                .quoteSettlement(true)
                .build();
        OrderBook orderBook = new OrderBook(btcusdt);
        
        // 使用OrderBook的单线程对象池创建买单
        for (int i = 1; i <= 10; i++) {
            Order order = orderBook.acquireAndInitOrder(
                    i, // id
                    1, // accountId
                    Order.OrderType.LIMIT,
                    Order.OrderSide.BID,
                    btcusdt.formatPrice(String.valueOf(10000-i)), // price
                    btcusdt.formatQuantity("0.01"), // quantity
                    btcusdt.formatPrice(String.valueOf(10000-i)) * btcusdt.formatQuantity("0.01") / btcusdt.getBase().getMultiplier(), // volume
                    btcusdt.formatPrice(String.valueOf(10000-i)) * btcusdt.formatQuantity("0.01") / btcusdt.getBase().getMultiplier(), // frozen
                    100, // takerRate
                    50   // makerRate
            );
            System.out.println("BID order result: " + orderBook.match(order));
        }
        
        // 使用OrderBook的单线程对象池创建卖单
        for (int i = 1; i <= 10; i++) {
            Order order = orderBook.acquireAndInitOrder(
                    i + 10, // id
                    1, // accountId
                    Order.OrderType.LIMIT,
                    Order.OrderSide.ASK,
                    btcusdt.formatPrice(String.valueOf(10000+i)), // price
                    btcusdt.formatQuantity("0.01"), // quantity
                    btcusdt.formatPrice(String.valueOf(10000+i)) * btcusdt.formatQuantity("0.01") / btcusdt.getBase().getMultiplier(), // volume
                    btcusdt.formatQuantity("0.01"), // frozen (对于卖单，冻结的是数量)
                    100, // takerRate
                    50   // makerRate
            );
            System.out.println("ASK order result: " + orderBook.match(order));
        }
        
        System.out.println("Final depth: " + orderBook.getDepth());
        
        // 打印单线程对象池统计信息
        System.out.println("Order pool stats: " + orderBook.getOrderPoolStats());
        System.out.println("Ticket pool stats: " + orderBook.getTicketPoolStats());
    }
}
