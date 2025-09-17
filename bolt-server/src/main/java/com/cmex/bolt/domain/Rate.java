package com.cmex.bolt.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Rate {

    /**
     * 手续费基础0.00001 = 10-5 十万分之一
     * 100 表示 0.00001 * 100 = 0.001 千分之一
     *
     * @return 基础利率
     */
    public static final long BASE_RATE = 100000;

    public static final double BASE_RATE_DOUBLE = 100000.0;
    
    public static final BigDecimal BASE_RATE_BD = new BigDecimal("100000");

    public static long getRate(long amount, long rate) {
        return Math.round(amount * rate / Rate.BASE_RATE_DOUBLE);
    }

    public static BigDecimal getRate(BigDecimal amount, BigDecimal rate) {
        return amount.multiply(rate).divide(BASE_RATE_BD, 18, RoundingMode.HALF_UP);
    }

    public static BigDecimal getRate(BigDecimal amount, int rate) {
        return amount.multiply(new BigDecimal(rate)).divide(BASE_RATE_BD, 18, RoundingMode.HALF_UP);
    }

}
