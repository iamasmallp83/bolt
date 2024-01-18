package com.cmex.bolt.spot;

import com.google.common.base.Stopwatch;
import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestPerformance {

    @Test
    public void testSync() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        Account account = new Account();
        CountDownLatch latch = new CountDownLatch(4);
        executor.execute(new TestSync(account, latch));
        executor.execute(new TestSync(account, latch));
        executor.execute(new TestSync(account, latch));
        executor.execute(new TestSync(account, latch));
        latch.await();
        System.out.println(account.getValue());
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 10000000; i++){
            account.deposit(1);
        }
        System.out.println(account.getValue());
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static class TestSync implements Runnable {


        private CountDownLatch latch;
        private Account account;

        public TestSync(Account account, CountDownLatch latch) {
            this.account = account;
            this.latch = latch;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = Stopwatch.createStarted();
            for (int i = 0; i < 2500000; i++) {
                account.syncDeposit(1L);
            }
            System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
            latch.countDown();
        }
    }

    private static class Account {
        private long value;

        public void deposit(long amount) {
            this.value += amount;
        }

        public synchronized void syncDeposit(long amount) {
            this.value += amount;
        }

        public long getValue() {
            return value;
        }
    }
}
