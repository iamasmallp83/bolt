package com.cmex.bolt.spot;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.BigDecimalUtil;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.GetAccountRequest;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.IncreaseRequest;

/**
 * Unit test for simple App.
 */
public class SpotTest {
    public static SpotServiceImpl service = new SpotServiceImpl();

    @BeforeAll
    public static void init() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(4);
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(1)
                .setCurrencyId(1)
                .setAmount("10000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(2)
                .setCurrencyId(2)
                .setAmount("100")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(3)
                .setCurrencyId(1)
                .setAmount("100")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("100")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(4)
                .setCurrencyId(3)
                .setAmount("20000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("20000000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(5)
                .setCurrencyId(1)
                .setAmount("10000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("10000000")));
            countDownLatch.countDown();
        }));
        service.increase(IncreaseRequest.newBuilder()
                .setRequestId(1)
                .setAccountId(6)
                .setCurrencyId(3)
                .setAmount("200000000000")
                .build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getData().getAvailable()), new BigDecimal("200000000000")));
            countDownLatch.countDown();
        }));
        countDownLatch.await();
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(1).getCurrency(), "USDT");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getAvailable()), new BigDecimal("10000")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(1).getFrozen()), BigDecimal.ZERO));
        }));
        service.getAccount(GetAccountRequest.newBuilder().setAccountId(2).build(), FakeStreamObserver.of(response -> {
            Assertions.assertEquals(response.getCode(), 1);
            Assertions.assertEquals(response.getDataMap().get(2).getCurrency(), "BTC");
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getAvailable()), new BigDecimal("100")));
            Assertions.assertTrue(BigDecimalUtil.eq(new BigDecimal(response.getDataMap().get(2).getFrozen()), BigDecimal.ZERO));
        }));
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
