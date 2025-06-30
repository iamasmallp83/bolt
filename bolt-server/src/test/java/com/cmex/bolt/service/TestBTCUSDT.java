package com.cmex.bolt.service;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.EnvoyUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TestBTCUSDT {
    public static EnvoyServer service = new EnvoyServer(BoltConfig.DEFAULT);

    @BeforeAll
    public static void before() {
        EnvoyUtil.increase(service, 1, 1,"10000");
        EnvoyUtil.increase(service, 2, 2,"10");
        EnvoyUtil.increase(service, 3, 1,"20000");
        EnvoyUtil.increase(service, 4, 2,"10");
        EnvoyUtil.increase(service, 5, 1,"10000");
    }

    @Test
    public void testCancel() throws InterruptedException {
        AtomicLong orderId = new AtomicLong();
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(2)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("1000")
                .setQuantity("2")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            orderId.set(response.getData().getId());
        }));
        Thread.sleep(100);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(1)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("1000")
                .setQuantity("1")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
        }));
        service.cancelOrder(Envoy.CancelOrderRequest.newBuilder().setOrderId(orderId.get()).build(), FakeStreamObserver.of(response -> {
            System.out.printf("cancel order id: %d\n", orderId.get());
        }));
        Thread.sleep(100);
        service.getDepth(Envoy.GetDepthRequest.newBuilder().setSymbolId(1).build(), FakeStreamObserver.of(
                response -> {
                    Assertions.assertEquals(0, response.getData().getAsksCount());
                    Assertions.assertEquals(0, response.getData().getBidsCount());
                }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("9000", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("1", response.getDataMap().get(2).getValue()));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(2).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("1000", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("9", response.getDataMap().get(2).getValue()));
        }));
    }

    /**
     * 买家 account 3 20000
     * 卖家 account 4 10
     * 买家taker 价格10000 1
     * 买家 手续费 10000 * 1 * 0.002 = 20
     * 卖家 手续费 10000 * 1 * 0.001 = 10
     * 买家
     * usdt 20000 - 10000 - 20 = 9980
     * btc 1
     * 买家
     * usdt 10000 - 10 = 9990
     * btc 9
     * @throws InterruptedException
     */
    @Test
    public void testMatch() throws InterruptedException {
        AtomicLong orderId = new AtomicLong();
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(4)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("10000")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            orderId.set(response.getData().getId());
        }));
        Thread.sleep(100);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(3)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("10000")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
        }));
        Thread.sleep(100);
        service.getDepth(Envoy.GetDepthRequest.newBuilder().setSymbolId(1).build(), FakeStreamObserver.of(
                response -> {
                    Assertions.assertEquals(0, response.getData().getAsksCount());
                    Assertions.assertEquals(0, response.getData().getBidsCount());
                }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(3).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("9980", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("1", response.getDataMap().get(2).getValue()));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(4).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("9990", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("9", response.getDataMap().get(2).getValue()));
        }));
    }

    /**
     * 扣除手续费金额不足
     * @throws InterruptedException
     */
    @Test
    public void testPlaceFailed() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(5)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("10000")
                .setQuantity("1")
                .setTakerRate(200)
                .build(), FakeStreamObserver.of(response -> {
            System.out.println(response);
            Assertions.assertTrue(response.getData().getId() == 0);
            latch.countDown();
        }));
        latch.await();
    }
}
