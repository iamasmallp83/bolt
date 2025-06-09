package com.cmex.bolt.server.util;

import java.math.BigDecimal;

public class BigDecimalUtil {

    public static boolean gt(BigDecimal one, BigDecimal two) {
        return one.compareTo(two) > 0;
    }

    public static boolean gte(BigDecimal one, BigDecimal two) {
        return one.compareTo(two) >= 0;
    }

    public static boolean lt(BigDecimal one, BigDecimal two) {
        return one.compareTo(two) < 0;
    }

    public static boolean lte(BigDecimal one, BigDecimal two) {
        return one.compareTo(two) <= 0;
    }

    public static boolean eq(BigDecimal one, BigDecimal two) {
        return one.compareTo(two) == 0;
    }

    public static boolean eq(String one, String two) {
        return new BigDecimal(one).compareTo(new BigDecimal(two)) == 0;
    }

    public static boolean gtZero(BigDecimal one) {
        return one.compareTo(BigDecimal.ZERO) > 0;
    }

    public static boolean gteZero(BigDecimal one) {
        return one.compareTo(BigDecimal.ZERO) > 0;
    }

    public static boolean eqZero(BigDecimal one) {
        return one.compareTo(BigDecimal.ZERO) == 0;
    }

    public static BigDecimal max(BigDecimal one, BigDecimal two) {
        return gte(one, two) ? one : two;
    }

    public static BigDecimal min(BigDecimal one, BigDecimal two) {
        return gte(one, two) ? two : one;
    }
}
