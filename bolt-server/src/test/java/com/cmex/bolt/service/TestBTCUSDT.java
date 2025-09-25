package com.cmex.bolt.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.EnvoyUtil;
import com.cmex.bolt.util.FakeStreamObserver;

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
        EnvoyUtil.placeOrder(service, 1, 1, 2, Envoy.Type.LIMIT, Envoy.Side.ASK, "1000", "2",
                FakeStreamObserver.of(response -> {
                    Assertions.assertTrue(response.getData().getId() > 0);
                    orderId.set(response.getData().getId());
                }));
        EnvoyUtil.placeOrder(service, 2, 1, 1, Envoy.Type.LIMIT, Envoy.Side.BID, "1000", "1",
                FakeStreamObserver.of(response -> {
                    Assertions.assertTrue(response.getData().getId() > 0);
                }));
        Thread.sleep(100);
        EnvoyUtil.cancelOrder(service, orderId.get(), FakeStreamObserver.of(response -> {
            System.out.println(response.getCode());
            System.out.printf("cancel order id: %d\n", orderId.get());
        }));
        Thread.sleep(100);
        EnvoyUtil.getDepth(service, 1, FakeStreamObserver.of(
                response -> {
                    Assertions.assertEquals(0, response.getData().getAsksCount());
                    Assertions.assertEquals(0, response.getData().getBidsCount());
                }));
        EnvoyUtil.getAccount(service, 1, FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("9000", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("1", response.getDataMap().get(2).getValue()));
        }));
        EnvoyUtil.getAccount(service, 2, FakeStreamObserver.of(response -> {
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
        EnvoyUtil.placeOrder(service, 1, 1, 4, Envoy.Type.LIMIT, Envoy.Side.ASK, "10000", "1",
                200, 100, FakeStreamObserver.of(response -> {
                    Assertions.assertTrue(response.getData().getId() > 0);
                    orderId.set(response.getData().getId());
                }));
        Thread.sleep(100);
        EnvoyUtil.placeOrder(service, 1, 1, 3, Envoy.Type.LIMIT, Envoy.Side.BID, "10000", "1",
                200, 100, FakeStreamObserver.of(response -> {
                    Assertions.assertTrue(response.getData().getId() > 0);
                }));
        Thread.sleep(100);
        EnvoyUtil.getDepth(service, 1, FakeStreamObserver.of(
                response -> {
                    Assertions.assertEquals(0, response.getData().getAsksCount());
                    Assertions.assertEquals(0, response.getData().getBidsCount());
                }));
        EnvoyUtil.getAccount(service, 3, FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq("9980", response.getDataMap().get(1).getValue()));
            Assertions.assertTrue(BigDecimalUtil.eq("1", response.getDataMap().get(2).getValue()));
        }));
        EnvoyUtil.getAccount(service, 4, FakeStreamObserver.of(response -> {
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
        EnvoyUtil.placeOrder(service, 1, 1, 5, Envoy.Type.LIMIT, Envoy.Side.BID, "10000", "1",
                200, 0, FakeStreamObserver.of(response -> {
                    System.out.println(response);
                    Assertions.assertTrue(response.getData().getId() == 0);
                    latch.countDown();
                }));
        latch.await();
    }
}
