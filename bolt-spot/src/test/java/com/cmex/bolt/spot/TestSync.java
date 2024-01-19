package com.cmex.bolt.spot;

import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestSync {

    @Test
    public void testSync() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        Account account = new Account();
        CountDownLatch latch = new CountDownLatch(4);
        //Mac mini Apple M2 Pro 32 GB
        //并发&锁
        //1030ms
        //1039ms
        //1042ms
        //1042ms
        //无锁&串行
        //2ms
        //MacBook Pro 2.8GHz i7 16GB
        //并发&锁
        //391
        //404
        //404
        //405
        //无锁&串行
        //9
        executor.execute(new TestSyncRunner(account, latch));
        executor.execute(new TestSyncRunner(account, latch));
        executor.execute(new TestSyncRunner(account, latch));
        executor.execute(new TestSyncRunner(account, latch));
        latch.await();
        System.out.println(account.getValue());
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 10000000; i++) {
            account.deposit(1);
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println(account.getValue());
    }

    @Test
    public void testSyncMultiAccount() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Account> accounts = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            accounts.add(new Account());
        }
        CountDownLatch latch = new CountDownLatch(4);
        //Mac mini Apple M2 Pro 32 GB
        //并发&锁
        //284ms
        //287ms
        //288ms
        //288ms
        //无锁&串行
        //12ms
        //MacBook Pro 2.8GHz i7 16GB
        //并发&锁
        //123
        //123
        //125
        //126
        //无锁&串行
        //34
        executor.execute(new TestSyncMultiAccountsRunner(accounts, latch));
        executor.execute(new TestSyncMultiAccountsRunner(accounts, latch));
        executor.execute(new TestSyncMultiAccountsRunner(accounts, latch));
        executor.execute(new TestSyncMultiAccountsRunner(accounts, latch));
        latch.await();
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 10000000; i++) {
            accounts.get(i % 256).deposit(1);
        }
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static class TestSyncRunner implements Runnable {


        private CountDownLatch latch;
        private Account account;

        public TestSyncRunner(Account account, CountDownLatch latch) {
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

    private static class TestSyncMultiAccountsRunner implements Runnable {


        private CountDownLatch latch;
        private List<Account> accounts;

        public TestSyncMultiAccountsRunner(List<Account> accounts, CountDownLatch latch) {
            this.accounts = accounts;
            this.latch = latch;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = Stopwatch.createStarted();
            for (int i = 0; i < 2500000; i++) {
                accounts.get(i % 256).syncDeposit(1L);
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
