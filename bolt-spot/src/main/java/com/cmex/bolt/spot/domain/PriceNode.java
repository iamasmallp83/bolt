package com.cmex.bolt.spot.domain;

import lombok.Getter;

import java.util.Iterator;
import java.util.LinkedHashSet;

@Getter
public class PriceNode {

    private final long price;

    private final LinkedHashSet<Order> orders;

    public PriceNode(long price, Order order) {
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

    public long getQuantity() {
        return orders.parallelStream().map(Order::getAvailableQuantity).reduce(0L, Long::sum);
    }

    public boolean done() {
        return orders.isEmpty();
    }

    @Override
    public String toString() {
        return "PriceNode{" +
                "price=" + price +
                ", quantity=" + getQuantity() +
                '}';
    }
}

