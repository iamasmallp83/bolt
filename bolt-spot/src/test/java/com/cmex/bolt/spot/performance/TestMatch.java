package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.cmex.bolt.spot.util.SpotServiceUtil;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;
import static com.cmex.bolt.spot.util.SpotServiceUtil.*;

/**
 * Unit test for simple App.
 */
public class TestMatch {
    private static SpotServiceImpl service = new SpotServiceImpl();

    @BeforeAll
    public static void init() {
        increase(service, 1, 1, "10000000");
        increase(service, 2, 2, "10000000");
        increase(service, 3, 3, "10000000");
        increase(service, 4, 4, "10000000");
    }

    @Test
    public void testOrder() throws InterruptedException {
        long times = 1000000;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(2);
        executor.submit(() -> {
            for (int i = 1; i <= times; i++) {
                if (i % 10 == 0) {
                    System.out.println("bid times = " + i);
                }
                placeOrder(1, 1, PlaceOrderRequest.Type.LIMIT, PlaceOrderRequest.Side.BID, "1", "1");
            }
            System.out.println("bid send done");
            latch.countDown();
        });
        executor.submit(() -> { 
            for (int i = 1; i <= times; i++) {
                if (i % 10 == 0) {
                    System.out.println("ask times = " + i);
                }
                placeOrder(1, 2, PlaceOrderRequest.Type.LIMIT, PlaceOrderRequest.Side.ASK, "1", "1");
            }
            System.out.println("ask send done");
            latch.countDown();
        });
        latch.await();
        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        TimeUnit.SECONDS.sleep(1);
        getAccount(service, 1, FakeStreamObserver.logger());
        getAccount(service, 2, FakeStreamObserver.logger());
        getDepth(service, 1);
        TimeUnit.SECONDS.sleep(1);
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

}
