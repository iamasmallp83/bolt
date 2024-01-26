package com.cmex.bolt.spot.domain;

import lombok.Getter;

import java.util.Iterator;
import java.util.LinkedHashSet;

@Getter
public class PriceNode {

    private final long price;

    private long quantity;

    private final LinkedHashSet<Order> orders;

    public PriceNode(long price, Order order) {
        this.price = price;
        this.orders = new LinkedHashSet<>();
        this.orders.add(order);
        this.quantity += order.getQuantity();
    }

    public void add(Order order) {
        this.orders.add(order);
        this.quantity += order.getAvailableQuantity();
    }

    public void remove(Order order) {
        this.orders.remove(order);
        this.quantity -= order.getAvailableQuantity();
    }

    public void decreaseQuantity(long amount) {
        this.quantity -= amount;
    }

    public Iterator<Order> iterator() {
        return orders.iterator();
    }

    public boolean isDone() {
        return orders.isEmpty();
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

