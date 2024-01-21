package com.cmex.bolt.spot.domain;

public class Rate {

    /**
     * 手续费基础0.00001 = 10-5,
     * 100 表示 0.00001 * 100 = 0.001 千分之一
     *
     * @return 基础利率
     */
    public static final int BASE_RATE = 100000;

    private int taker;

    private int maker;

    public int getTaker() {
        return taker / BASE_RATE;
    }

    public int getMaker() {
        return maker / BASE_RATE;
    }
}
