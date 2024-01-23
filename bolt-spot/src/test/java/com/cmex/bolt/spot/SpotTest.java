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

    @BeforeAll
    public static void init() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(2);
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
