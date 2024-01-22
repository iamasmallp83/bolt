package com.cmex.bolt.spot;

import org.decimal4j.immutable.Decimal4f;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Unit test for simple App.
 */
public class SpotTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldDecimal4j() {
        Decimal4f decimal4f = Decimal4f.valueOf(1.01);
        System.out.println(decimal4f);
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        long factor = (long) Math.pow(10, places);
        value = value * factor;
        long tmp = Math.round(value);
        return (double) tmp / factor;
    }

    public static double roundByDecimal(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
