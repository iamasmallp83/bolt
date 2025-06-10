package com.cmex.bolt;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.EnvoyUtil;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

/**
 * Unit test for simple App.
 */
public class BoltTest {
    public static EnvoyServer service = new EnvoyServer();

    @BeforeAll
    public static void init() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(4);
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(1)
                .setCurrencyId(1)
                .setAmount("10000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000")));
            countDownLatch.countDown();
        }));
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(2)
                .setCurrencyId(2)
                .setAmount("100")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(3)
                .setCurrencyId(1)
                .setAmount("100")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(4)
                .setCurrencyId(3)
                .setAmount("20000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("20000000")));
            countDownLatch.countDown();
        }));
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(5)
                .setCurrencyId(1)
                .setAmount("10000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000000")));
            countDownLatch.countDown();
        }));
        service.increase(Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(6)
                .setCurrencyId(3)
                .setAmount("200000000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("200000000000")));
            countDownLatch.countDown();
        }));
        EnvoyUtil.increase(service, 11, 1, "1");
        EnvoyUtil.increase(service, 12, 2, "1");
        countDownLatch.await();
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(1).getCurrency(), "USDT");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getAvailable()), new BigDecimal("10000")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getFrozen()), BigDecimal.ZERO));
        }));
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(2).build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(2).getCurrency(), "BTC");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getAvailable()), new BigDecimal("100")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getFrozen()), BigDecimal.ZERO));
        }));
    }

}
