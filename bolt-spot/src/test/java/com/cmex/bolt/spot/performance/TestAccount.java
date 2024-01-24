package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;

/**
 * Unit test for simple App.
 */
public class TestAccount {
    private static SpotServiceImpl service = new SpotServiceImpl();

    @Test
    public void testIncrease() throws InterruptedException {
        long times = 1_000_000;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> {
            for (int i = 1; i <= times; i++) {
                increase(1, 1, "1");
            }
            latch.countDown();
        });
        latch.await();
        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        TimeUnit.SECONDS.sleep(1);
        getAccount(1, FakeStreamObserver.logger());
        executor.shutdown();
    }

    public static void placeOrder(int symbolId, int accountId, PlaceOrderRequest.Type type, PlaceOrderRequest.Side side,
                                  String price, String quantity) {
        placeOrder(symbolId, accountId, type, side, price, quantity, FakeStreamObserver.noop());
    }

    public static void placeOrder(int symbolId, int accountId, PlaceOrderRequest.Type type, PlaceOrderRequest.Side side,
                                  String price, String quantity, FakeStreamObserver<SpotServiceProto.PlaceOrderResponse> observer) {
        service.placeOrder(PlaceOrderRequest.newBuilder()
                .setRequestId(1)
                .setSymbolId(symbolId)
                .setAccountId(accountId)
                .setType(type)
                .setSide(side)
                .setPrice(price)
                .setQuantity(quantity)
                .build(), observer);
    }

    public static <T> void increase(int accountId, int currencyId, String amount, FakeStreamObserver<SpotServiceProto.IncreaseResponse> observer) {
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setAccountId(accountId)
                .setCurrencyId(currencyId)
                .setAmount(amount)
                .build(), observer);
    }

    public static void increase(int accountId, int currencyId, String amount) {
        increase(accountId, currencyId, amount, FakeStreamObserver.noop());
    }

    public static <T> void getAccount(int accountId, FakeStreamObserver<T> observer) {
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), FakeStreamObserver.logger());
    }

    public static void getDepth(int symbolId) {
        service.getDepth(SpotServiceProto.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }
}
