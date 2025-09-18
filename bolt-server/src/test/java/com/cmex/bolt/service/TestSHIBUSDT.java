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

public class TestSHIBUSDT {

    public static EnvoyServer service = new EnvoyServer(BoltConfig.DEFAULT);

    @BeforeAll
    public static void before() {
        EnvoyUtil.increase(service, 3, 1,"86");
        EnvoyUtil.increase(service, 4, 3,"10000000");
        EnvoyUtil.increase(service, 5, 1,"10000000");
        EnvoyUtil.increase(service, 6, 3,"200000000000");
    }

    @Test
    public void testShibUsdt() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(4)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(3)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("0.00000860")
                .setQuantity("10000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        Thread.sleep(1000);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(4).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "86"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "0"));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(3).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "10000000"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "0"));
        }));
    }


    /**
     * Account 5 初始资产 10_000_000 usdt
     * Account 6 初始资产 200_000_000_000   shib
     * 6 卖单 0.00000860, 10_000_000_000  0.1%
     * 5 买单 0.00000864，8_000_000_000  0.2%
     * Account 5 资产 9931062.4 usdt 8_000_000_000 shib
     * Account 6 资产 68731.2 usdt 190_000_000_000 shib frozen 2_000_000_000
     *
     * @throws InterruptedException
     */
    @Test
    public void testShibUsdt1() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(6)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(5)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("0.00000864")
                .setQuantity("8000000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        Thread.sleep(1000);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(5).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9931062.4"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "8000000000"));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(6).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "68731.2"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "190000000000"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getFrozen(), "2000000000"));
        }));
    }
}
