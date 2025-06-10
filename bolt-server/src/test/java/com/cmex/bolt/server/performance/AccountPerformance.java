package com.cmex.bolt.server.performance;

import com.cmex.bolt.server.grpc.EnvoyServer;
import com.cmex.bolt.server.grpc.Envoy;
import com.cmex.bolt.server.util.BigDecimalUtil;
import com.cmex.bolt.server.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cmex.bolt.server.grpc.Envoy.PlaceOrderRequest;
import static com.cmex.bolt.server.util.EnvoyUtil.*;

/**
 * Unit test for simple App.
 */
public class AccountPerformance {
    private static final EnvoyServer service = new EnvoyServer();

    @Test
    public void testIncrease() throws InterruptedException {
        int times = 2_000_000;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> {
            for (int i = 1; i <= times; i++) {
                increase(service, i, 1, "1");
            }
            latch.countDown();
        });
        latch.await();
        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        AtomicBoolean running = new AtomicBoolean(true);
        while (running.get()) {
            getAccount(service, times, new FakeStreamObserver<>(response -> {
                Envoy.Balance balance = response.getDataMap().get(1);
                if (balance != null) {
                    running.set(!BigDecimalUtil.eq(balance.getAvailable(), "1"));
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        executor.shutdown();
    }

    public static void placeOrder(int symbolId, int accountId, PlaceOrderRequest.Type type, PlaceOrderRequest.Side side,
                                  String price, String quantity) {
        placeOrder(symbolId, accountId, type, side, price, quantity, FakeStreamObserver.noop());
    }

    public static void placeOrder(int symbolId, int accountId, PlaceOrderRequest.Type type, PlaceOrderRequest.Side side,
                                  String price, String quantity, FakeStreamObserver<Envoy.PlaceOrderResponse> observer) {
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
}