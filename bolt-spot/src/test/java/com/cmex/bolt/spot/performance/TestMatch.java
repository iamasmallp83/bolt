package com.cmex.bolt.spot.performance;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderRequest;

/**
 * Unit test for simple App.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMatch {
    private SpotServiceImpl service = new SpotServiceImpl();

    @BeforeAll
    public void init() {
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(1)
                .setCurrencyId(1)
                .setAmount("10000000")
                .build(), FakeStreamObserver.noop());
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(2)
                .setCurrencyId(2)
                .setAmount("10000000")
                .build(), FakeStreamObserver.noop());
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(3)
                .setCurrencyId(3)
                .setAmount("10000000")
                .build(), FakeStreamObserver.noop());
    }

    @Test
    public void testBtcUsdt() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(2);
        executor.submit(() -> {
            for (int i = 1; i <= 1_000_000; i++) {
                PlaceOrderRequest request = PlaceOrderRequest.newBuilder()
                        .setRequestId(1)
                        .setSymbolId(1)
                        .setAccountId(1)
                        .setType(PlaceOrderRequest.Type.LIMIT)
                        .setSide(PlaceOrderRequest.Side.BID)
                        .setPrice("1")
                        .setQuantity("1")
                        .build();
                if (i == 1 || i == 1_000_000) {
                    service.placeOrder(request, FakeStreamObserver.logger());
                } else {
                    service.placeOrder(request, FakeStreamObserver.noop());
                }
            }
            System.out.println("btc send done");
            latch.countDown();
        });
        executor.submit(() -> {
            for (int i = 1; i <= 1_000_000; i++) {
                PlaceOrderRequest request = PlaceOrderRequest.newBuilder()
                        .setRequestId(1)
                        .setSymbolId(2)
                        .setAccountId(3)
                        .setType(PlaceOrderRequest.Type.LIMIT)
                        .setSide(PlaceOrderRequest.Side.ASK)
                        .setPrice("1")
                        .setQuantity("1")
                        .build();
                if (i == 1 || i == 1_000_000) {
                    service.placeOrder(request, FakeStreamObserver.logger());
                } else {
                    service.placeOrder(request, FakeStreamObserver.noop());
                }
            }
            System.out.println("shib send done");
            latch.countDown();
        });
        latch.await();
        System.out.println("elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        getAccount(1, FakeStreamObserver.logger());
        getAccount(3, FakeStreamObserver.logger());
        TimeUnit.SECONDS.sleep(1);
    }

    protected <T> void increase(int accountId, int currencyId, String amount, FakeStreamObserver<T> observer) {
        service.increase(SpotServiceProto.IncreaseRequest.newBuilder()
                .setAccountId(accountId)
                .setCurrencyId(currencyId)
                .setAmount(amount)
                .build(), FakeStreamObserver.logger());
    }

    protected void increase(int accountId, int currencyId, String amount) {
        increase(accountId, currencyId, amount, FakeStreamObserver.logger());
    }

    protected <T> void getAccount(int accountId, FakeStreamObserver<T> observer) {
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build(), FakeStreamObserver.logger());
    }

    protected void getDepth(int symbolId) {
        service.getDepth(SpotServiceProto.GetDepthRequest.newBuilder()
                .setSymbolId(symbolId)
                .build(), FakeStreamObserver.logger());
    }
}
