package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.cmex.bolt.spot.util.SpotServiceUtil;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;
import static com.cmex.bolt.spot.util.SpotServiceUtil.*;

/**
 * Unit test for simple App.
 */
public class TestMatch {
    private static SpotServiceImpl service = new SpotServiceImpl();

    @BeforeAll
    public static void init() {
        increase(service, 1, 1, "500000");
        increase(service, 2, 2, "500000");
        increase(service, 3, 3, "10000000");
        increase(service, 4, 4, "10000000");
    }

    @Test
    public void testOrder() throws InterruptedException {
        long times = 10;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(2);
        executor.submit(() -> {
            for (int i = 1; i <= times; i++) {
                placeOrder(1, 1, PlaceOrderRequest.Type.LIMIT, PlaceOrderRequest.Side.BID, "1", "1");
            }
            System.out.println("bid send done");
            latch.countDown();
        });
        executor.submit(() -> {
            for (int i = 1; i <= times; i++) {
                placeOrder(1, 2, PlaceOrderRequest.Type.LIMIT, PlaceOrderRequest.Side.ASK, "1", "1");
            }
            System.out.println("ask send done");
            latch.countDown();
        });
        latch.await();
        System.out.println("send elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        while (true) {
            AtomicReference<Boolean> account1Eq = new AtomicReference<>(false);
            AtomicReference<Boolean> account2Eq = new AtomicReference<>(false);
            getAccount(service, 1, new FakeStreamObserver<>((response) -> {
                if (response.getDataMap().get(1).getAvailable().equals("0.0")
                        && response.getDataMap().get(2).getAvailable().equals("500000.0")) {
                    account1Eq.set(true);
                }
            }));
            getAccount(service, 2, new FakeStreamObserver<>((response) -> {
                if (response.getDataMap().get(1).getAvailable().equals("500000.0")
                        && response.getDataMap().get(2).getAvailable().equals("0.0")) {
                    account2Eq.set(true);
                }
            }));
            if (account1Eq.get() && account2Eq.get()) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(500);
        }
        System.out.println("get account elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        getDepth(service, 1);
        System.out.println("get depth elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
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
