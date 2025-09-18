package com.cmex.bolt.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class TestCurrency {

    @Test
    public void test(){
        Currency usdt = Currency.builder()
                .id(1)
                .name("USDT")
                .precision(4)
                .build();
        Currency btc = Currency.builder()
                .id(2)
                .name("BTC")
                .precision(8)
                .build();
    }
}
