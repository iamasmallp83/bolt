package com.cmex.bolt.domain;

public class Rate {

    /**
     * 手续费基础0.00001 = 10-5 十万分之一
     * 100 表示 0.00001 * 100 = 0.001 千分之一
     *
     * @return 基础利率
     */
    public static final long BASE_RATE = 100000;

    public static final double BASE_RATE_DOUBLE = 100000.0;

    public static long getRate(long amount, long rate) {
        return Math.round(amount * rate / Rate.BASE_RATE_DOUBLE);
    }


}
