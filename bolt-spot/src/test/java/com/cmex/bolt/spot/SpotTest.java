package com.cmex.bolt.spot;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.util.BigDecimalUtil;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.*;

/**
 * Unit test for simple App.
 */
public class SpotTest {
    private static SpotServiceImpl service = new SpotServiceImpl();
    @BeforeAll
    public static void init() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(4);
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(1)
                .setCurrencyId(1)
                .setAmount("10000")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(2)
                .setCurrencyId(2)
                .setAmount("100")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(3)
                .setCurrencyId(1)
                .setAmount("100")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(4)
                .setCurrencyId(3)
                .setAmount("20000000")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("20000000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(5)
                .setCurrencyId(1)
                .setAmount("10000000")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(6)
                .setCurrencyId(3)
                .setAmount("200000000000")
                .build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("200000000000")));
            countDownLatch.countDown();
        }));
        countDownLatch.await();
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(1).build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(1).getCurrency(), "USDT");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getAvailable()), new BigDecimal("10000")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getFrozen()), BigDecimal.ZERO));
        }));
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(2).build(), create(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(2).getCurrency(), "BTC");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getAvailable()), new BigDecimal("100")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getFrozen()), BigDecimal.ZERO));
        }));
    }

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
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(1)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.BID)
                .setPrice("10")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(1)
                .setAccountId(2)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.ASK)
                .setPrice("10")
                .setQuantity("1")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(2).build(), create(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9.98"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "99"));
        }));
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(1).build(), create(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9989.99"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(2).getAvailable(), "1"));
        }));
    }


    /**
     * Account 3 初始资产 100 usdt
     * Account 4 初始资产 20000000   shib
     * 4 卖单 0.00000860, 10000000  0.1%
     * 3 买单 0.00000864，10000000  0.2%
     * Account 3 资产 13.828 usdt 10000000 shib
     * Account 4 资产 85.914 10000000 shib
     * @throws InterruptedException
     */
    @Test
    public void testShibUsdt() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(4)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(3)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.BID)
                .setPrice("0.00000864")
                .setQuantity("10000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(4).build(), create(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "85.914"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "10000000"));
        }));
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(3).build(), create(response -> {
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
     * @throws InterruptedException
     */
    @Test
    public void testShibUsdt1() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(6)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.ASK)
                .setPrice("0.00000860")
                .setQuantity("10000000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(2)
                .setAccountId(5)
                .setType(PlaceOrderRequest.Type.LIMIT)
                .setSide(PlaceOrderRequest.Side.BID)
                .setPrice("0.00000864")
                .setQuantity("8000000000")
                .setTakerRate(200)
                .setMakerRate(100)
                .build(), create(response -> {
            Assertions.assertTrue(response.getData().getId() > 0);
            latch.countDown();
        }));
        latch.await();
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(5).build(), create(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "9931062.4"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "8000000000"));
        }));
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(6).build(), create(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "68731.2"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getAvailable(), "190000000000"));
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(3).getFrozen(), "2000000000"));
        }));
    }

    private static <T> FakeStreamObserver<T> create(Consumer<T> consumer) {
        return new FakeStreamObserver<T>(consumer);
    }

    private static class FakeStreamObserver<T> implements StreamObserver<T> {

        private final Consumer<T> consumer;

        public FakeStreamObserver(Consumer<T> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onNext(T t) {
            consumer.accept(t);
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {
        }
    }
}
