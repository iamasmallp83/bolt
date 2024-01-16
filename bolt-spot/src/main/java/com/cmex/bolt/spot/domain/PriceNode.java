package com.cmex.bolt.spot.domain;

import lombok.Getter;

import java.math.BigDecimal;
import java.util.LinkedHashSet;

@Getter
public class PriceNode {

    private final BigDecimal price;

    private BigDecimal total;

    private final LinkedHashSet<Order> orders;

    public PriceNode(BigDecimal price, Order order) {
        this.price = price;
        this.orders = new LinkedHashSet<>();
        this.orders.add(order);
        this.total = order.getSize();
    }

    public void add(Order order) {
        this.orders.add(order);
        this.total = this.total.add(order.getSize());
    }

    public void remove(Order order) {
        this.orders.remove(order);
        this.total = this.total.subtract(order.getSize());
    }

}

