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
                    .price(new BigDecimal(100 - i))
                    .quantity(new BigDecimal("1"))
                    .availableQuantity(new BigDecimal("1"))
                    .build();
            System.out.println(orderBook.match(order));
        }
        for (int i = 0; i < 5; i++) {
            Order order = Order.builder()
                    .type(Order.OrderType.LIMIT)
                    .side(Order.OrderSide.ASK)
                    .price(new BigDecimal(100 + i))
                    .quantity(new BigDecimal("2"))
                    .availableQuantity(new BigDecimal("2"))
                    .build();
            System.out.println(orderBook.match(order));
        }
        System.out.println(orderBook.getDepth());
    }
}
