package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.util.BigDecimalUtil;
import lombok.Getter;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.LinkedHashSet;

@Getter
public class PriceNode {

    private final BigDecimal price;

    private final LinkedHashSet<Order> orders;

    public PriceNode(BigDecimal price, Order order) {
        this.price = price;
        this.orders = new LinkedHashSet<>();
        this.orders.add(order);
    }

    public void add(Order order) {
        this.orders.add(order);
    }

    public void remove(Order order) {
        this.orders.remove(order);
    }

    public Iterator<Order> iterator() {
        return orders.iterator();
    }

    public boolean isDone() {
        return orders.isEmpty();
    }

    public BigDecimal getQuantity() {
        return orders.parallelStream().map(Order::getAvailableQuantity).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    @Override
    public String toString() {
        return "PriceNode{" +
                "price=" + price +
                ", quantity=" + getQuantity() +
                '}';
    }
}

