package com.cmex.bolt.spot.domain;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOrder {

    @Test
    public void test() {
        Order order = Order.builder()
                .symbol(Symbol.getSymbol((short) 1))
                .id(1)
                .accountId(1)
                .type(Order.OrderType.LIMIT)
                .side(Order.OrderSide.BID)
                .price(100)
                .quantity(1)
                .build();
        Assertions.assertEquals(100L, order.getAvailableVolume());
        Assertions.assertEquals(order.getVolume(), order.getAvailableVolume());
    }
}
