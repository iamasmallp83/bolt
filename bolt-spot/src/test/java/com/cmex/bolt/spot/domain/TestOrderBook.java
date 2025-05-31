package com.cmex.bolt.spot.domain;

import org.junit.jupiter.api.Test;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        OrderBook orderBook = new OrderBook(Symbol.builder().build());
        for (int i = 0; i < 5; i++) {
            Order order = Order.builder()
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.BID)
                    .price(100 - i)
                    .quantity(1)
                    .build();
            System.out.println(orderBook.match(order));
        }
        for (int i = 0; i < 5; i++) {
            Order order = Order.builder()
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.ASK)
                    .price(100 + i)
                    .quantity(2)
                    .build();
            System.out.println(orderBook.match(order));
        }
        System.out.println(orderBook.getDepth());
    }
}
