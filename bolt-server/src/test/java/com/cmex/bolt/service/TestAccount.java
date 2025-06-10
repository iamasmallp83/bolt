package com.cmex.bolt.service;

import com.cmex.bolt.BoltTest;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.FakeStreamObserver;
import com.cmex.bolt.Envoy;
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
