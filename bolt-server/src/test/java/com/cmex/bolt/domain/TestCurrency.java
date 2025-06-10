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
        long price = usdt.parse("10000");
        long quantity = usdt.parse("2");
        System.out.println(Long.MAX_VALUE);
        BigDecimal result =  new BigDecimal(price).multiply(new BigDecimal(quantity));
        System.out.println(result);
        System.out.printf("%s*%s=%s\n", price , quantity, result);
        System.out.printf("%d*%d=%d\n", price , quantity, Math.multiplyExact(price, quantity));

    }
}
