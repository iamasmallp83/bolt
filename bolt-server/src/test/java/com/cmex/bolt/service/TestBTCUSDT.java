package com.cmex.bolt.service;

import com.cmex.bolt.BoltTest;
import com.cmex.bolt.Envoy;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TestBTCUSDT extends BoltTest {

    @Test
    public void testSimple() throws InterruptedException {
        AtomicLong orderId = new AtomicLong();
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(20000)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("1000")
                .setQuantity("2")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            orderId.set(response.getData().getId());
        }));
        Thread.sleep(100);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(20000).build(), FakeStreamObserver.of(response -> {
            System.out.println("before match account 20000:");
            System.out.println(response.getDataMap());
        }));
        Thread.sleep(100);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(10000)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("1000")
                .setQuantity("1")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
        }));
        Thread.sleep(100);
        System.out.println("after match");
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(10000).build(), FakeStreamObserver.of(response -> {
            System.out.println("account 10000:");
            System.out.println(response.getDataMap());
//            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "5000"));
//            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "0.5"));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(20000).build(), FakeStreamObserver.of(response -> {
            System.out.println("account 20000:");
            System.out.println(response.getDataMap());
//            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "5000"));
//            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "0.5"));
        }));
        Thread.sleep(1000);
        service.getDepth(Envoy.GetDepthRequest.newBuilder().setSymbolId(1).build(), FakeStreamObserver.of(System.out::println));
        Thread.sleep(1000);
        service.cancelOrder(Envoy.CancelOrderRequest.newBuilder().setOrderId(orderId.get()).build(), FakeStreamObserver.noop());
        Thread.sleep(1000);
        service.getDepth(Envoy.GetDepthRequest.newBuilder().setSymbolId(1).build(), FakeStreamObserver.of(
                response -> {
                    System.out.println("after cancel:");
                    System.out.println(response.getData());
                }));
        Thread.sleep(1000);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(10000).build(), FakeStreamObserver.of(response -> {
            System.out.println("account 10000:");
            System.out.println(response.getDataMap());
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(20000).build(), FakeStreamObserver.of(response -> {
            System.out.println("account 20000:");
            System.out.println(response.getDataMap());
        }));
    }

    @Test
    public void testBtcUsdt() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(1)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("10")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        final CountDownLatch takerLatch = new CountDownLatch(1);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(2)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("10")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            takerLatch.countDown();
        }));
        takerLatch.await();
        Thread.sleep(1000);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(2).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9.98"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "99"));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9989.99"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "1"));
        }));
    }
}
