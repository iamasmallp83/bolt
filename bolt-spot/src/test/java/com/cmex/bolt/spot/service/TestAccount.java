package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.SpotTest;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.cmex.bolt.spot.util.BigDecimalUtil;
import com.cmex.bolt.spot.util.FakeStreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class TestAccount extends SpotTest {

    @Test
    public void testIncrease() {
        service.getAccount(SpotServiceProto.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "10000"));
        }));
    }
}
