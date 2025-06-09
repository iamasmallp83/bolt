package com.cmex.bolt.server.service;

import com.cmex.bolt.server.SpotTest;
import com.cmex.bolt.server.util.BigDecimalUtil;
import com.cmex.bolt.server.util.FakeStreamObserver;
import com.cmex.bolt.server.grpc.Bolt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class TestAccount extends SpotTest {

    @Test
    public void testIncrease() {
        service.getAccount(Bolt.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "10000"));
        }));
    }
}
