package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.api.PlaceOrder;
import com.cmex.bolt.spot.domain.Order;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCreateOrder {

    private final int times = 1_000_000;

    @Test
    public void testCreateOrder() {
//        Stopwatch stopwatch = Stopwatch.createStarted();
//        List<Order> orders = new ArrayList<Order>(times);
//        for (int i = 0; i < times; i++) {
//            Order order = Order.builder().build();
//            orders.add(order);
//        }
//        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
//        List<PlaceOrder> placeOrders = new ArrayList<PlaceOrder>(times);
//        for (int i = 0; i < times; i++) {
//            PlaceOrder order = new PlaceOrder();
//            placeOrders.add(order);
//        }
//        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
