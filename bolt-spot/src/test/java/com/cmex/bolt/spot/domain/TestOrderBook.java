package com.cmex.bolt.spot.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        OrderBook orderBook = new OrderBook(Symbol.getSymbol((short) 1));
        for (int i = 0; i < 5; i++) {
            Order order = Order.builder()
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.BID)
                    .price(100 - i)
                    .quantity(1)
                    .availableQuantity(1)
                    .build();
            System.out.println(orderBook.match(order));
        }
        for (int i = 0; i < 5; i++) {
            Order order = Order.builder()
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.ASK)
                    .price(100)
                    .quantity(2)
                    .availableQuantity(2)
                    .build();
            System.out.println(orderBook.match(order));
        }
        System.out.println(orderBook.getDepth());
    }
}
