package com.cmex.bolt.service;

import com.cmex.bolt.BoltTest;
import com.cmex.bolt.Envoy;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class TestBTCUSDTMatch extends BoltTest {

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
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(1)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.BID)
                .setPrice("1")
                .setQuantity("1")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(2)
                .setType(Envoy.Type.LIMIT)
                .setSide(Envoy.Side.ASK)
                .setPrice("1")
                .setQuantity("1")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        Thread.sleep(1000);
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9999"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "1"));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(2).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "1"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "99"));
        }));
    }
}
