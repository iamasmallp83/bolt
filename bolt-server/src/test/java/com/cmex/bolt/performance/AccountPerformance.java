package com.cmex.bolt.performance;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.cmex.bolt.util.EnvoyUtil.getAccount;
import static com.cmex.bolt.util.EnvoyUtil.increase;

/**
 * Unit test for simple App.
 */
public class AccountPerformance {
    private static final EnvoyServer service = new EnvoyServer();

    @Test
    public void testIncrease() throws InterruptedException {
        int times = 50_000;
        int threadCount = 16;
        System.out.println("total request : " + times * threadCount + " threads :" + threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Stopwatch stopwatch = Stopwatch.createStarted();
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int count = 1; count <= threadCount; count++) {
            int finalCount = count;
            executor.submit(() -> {
                for (int i = 1; i <= times; i++) {
                    increase(service, i, (finalCount % 4) + 1, "1");
                }
                latch.countDown();
            });
        }
        latch.await();
        System.out.println("send elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        AtomicBoolean running = new AtomicBoolean(true);
        while (running.get()) {
            getAccount(service, times, new FakeStreamObserver<>(response -> {
                Envoy.Balance balance = response.getDataMap().get(4);
                if (balance != null) {
                    running.set(!BigDecimalUtil.eq(balance.getAvailable(), "4"));
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        System.out.println("total elapsed : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        executor.shutdown();
    }
}