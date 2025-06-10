package com.cmex.bolt.server.service;

import com.cmex.bolt.server.BoltTest;
import com.cmex.bolt.server.grpc.Envoy;
import com.cmex.bolt.server.util.BigDecimalUtil;
import com.cmex.bolt.server.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class TestBTCUSDT extends BoltTest {

    /**
     * Account 1 初始资产 10000 usdt
     * Account 2 初始资产 100   btc
     * 1 买单 10，1  0.1%
     * 2 卖单 10，1  0.2%
     * Account 1 资产 btc 1 usdt 9989.99
     * Account 2 资产 btc 99 usdt 9.98
     *
     * @throws InterruptedException
     */
    @Test
    public void testBtcUsdt() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(1)
                .setType(Envoy.PlaceOrderRequest.Type.LIMIT)
                .setSide(Envoy.PlaceOrderRequest.Side.BID)
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
                .setType(Envoy.PlaceOrderRequest.Type.LIMIT)
                .setSide(Envoy.PlaceOrderRequest.Side.ASK)
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
