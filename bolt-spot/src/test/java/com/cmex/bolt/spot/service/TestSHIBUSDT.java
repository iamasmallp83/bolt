package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.SpotTest;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.BigDecimalUtil;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class TestSHIBUSDT extends SpotTest {

    /**
     * Account 3 初始资产 100 usdt
     * Account 4 初始资产 20000000   shib
     * 4 卖单 0.00000860, 10000000  0.1%
     * 3 买单 0.00000864，10000000  0.2%
     * Account 3 资产 13.828 usdt 10000000 shib
     * Account 4 资产 85.914 10000000 shib
     *
     * @throws InterruptedException
     */
    @Test
    public void testShibUsdt() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(SpotServiceProto.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(4)
                .setType(SpotServiceProto.PlaceOrderRequest.Type.LIMIT)
                .setSide(SpotServiceProto.PlaceOrderRequest.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(SpotServiceProto.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(3)
                .setType(SpotServiceProto.PlaceOrderRequest.Type.LIMIT)
                .setSide(SpotServiceProto.PlaceOrderRequest.Side.BID)
                .setPrice("0.00000864")
                .setQuantity("10000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        Thread.sleep(1000);
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder().setAccountId(4).build(), FakeStreamObserver.of(response -> {
            System.out.println(response.getDataMap().get(1));
            System.out.println(response.getDataMap().get(3));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "85.914"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "10000000"));
        }));
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder().setAccountId(3).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "13.828"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "10000000"));
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
        service.placeOrder(SpotServiceProto.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(6)
                .setType(SpotServiceProto.PlaceOrderRequest.Type.LIMIT)
                .setSide(SpotServiceProto.PlaceOrderRequest.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(SpotServiceProto.PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(5)
                .setType(SpotServiceProto.PlaceOrderRequest.Type.LIMIT)
                .setSide(SpotServiceProto.PlaceOrderRequest.Side.BID)
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
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder().setAccountId(5).build(), FakeStreamObserver.of(response -> {
            System.out.println(response.getDataMap().get(1));
            System.out.println(response.getDataMap().get(3));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9931062.4"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "8000000000"));
        }));
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder().setAccountId(6).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "68731.2"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "190000000000"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getFrozen(), "2000000000"));
        }));
    }
}
