package com.cmex.bolt.server.service;

import com.cmex.bolt.server.BoltTest;
import com.cmex.bolt.server.util.BigDecimalUtil;
import com.cmex.bolt.server.util.FakeStreamObserver;
import com.cmex.bolt.server.grpc.Envoy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class TestAccount extends BoltTest {

    @Test
    public void testIncrease() {
        service.getAccount(Envoy.GetAccountRequest.newBuilder().setAccountId(1).build(), FakeStreamObserver.of(response -> {
            Assertions.assertTrue(BigDecimalUtil.eq(response.getDataMap().get(1).getAvailable(), "10000"));
        }));
    }
}
