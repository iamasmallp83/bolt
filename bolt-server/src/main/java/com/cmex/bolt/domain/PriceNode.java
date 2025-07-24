package com.cmex.bolt.domain;

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
        this.quantity += order.getAvailableQuantity();
    }

    public void add(Order order) {
        this.orders.add(order);
        this.quantity += order.getAvailableQuantity();
    }

    public void remove(Order order) {
        this.orders.remove(order);
        this.quantity -= order.getAvailableQuantity();
    }

    /**
     * 仅从订单集合中移除订单，不更新数量
     * 用于已经通过iterator.remove()删除的情况
     */
    public void removeWithoutQuantityUpdate(Order order) {
        // 订单已经通过iterator.remove()删除，这里不需要再次删除
        // 只需要更新数量
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

