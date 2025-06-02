package com.cmex.bolt.spot.domain;

import org.junit.jupiter.api.Test;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        Symbol btcusdt = Symbol.builder()
                .id(1)
                .name("BTC/USDT")
                .base(Currency.builder().name("BTC").precision(6).build())
                .quote(Currency.builder().name("USDT").precision(4).build())
                .quoteSettlement(true)
                .build();
        OrderBook orderBook = new OrderBook(btcusdt);
        for (int i = 1; i <= 10; i++) {
            Order order = Order.builder()
                    .symbol(orderBook.getSymbol())
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.BID)
                    .price(btcusdt.formatPrice(String.valueOf(10000-i)))
                    .quantity(btcusdt.formatQuantity("0.01"))
                    .build();
            System.out.println(orderBook.match(order));
        }
        for (int i = 1; i <= 10; i++) {
            Order order = Order.builder()
                    .symbol(orderBook.getSymbol())
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.ASK)
                    .price(btcusdt.formatPrice(String.valueOf(10000+i)))
                    .quantity(btcusdt.formatQuantity("0.01"))
                    .build();
            System.out.println(orderBook.match(order));
        }
        System.out.println(orderBook.getDepth());
    }
}
