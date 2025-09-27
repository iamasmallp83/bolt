package com.cmex.bolt.util;

import java.math.BigDecimal;

public class BigDecimalUtil {

    public static boolean gt(BigDecimal one, BigDecimal other) {
        return one.compareTo(other) > 0;
    }

    public static boolean gte(BigDecimal one, BigDecimal other) {
        return one.compareTo(other) >= 0;
    }

    public static boolean lt(BigDecimal one, BigDecimal other) {
        return one.compareTo(other) < 0;
    }

    public static boolean lte(BigDecimal one, BigDecimal other) {
        return one.compareTo(other) <= 0;
    }

    public static boolean eq(BigDecimal one, BigDecimal other) {
        return one.compareTo(other) == 0;
    }

    public static boolean eq(String one, String other) {
        if (new BigDecimal(one).compareTo(new BigDecimal(other)) == 0) {
            return true;
        }
        System.out.println("one is " + one + ", other is " + other);
        return false;
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

    public static BigDecimal max(BigDecimal one, BigDecimal other) {
        return gte(one, other) ? one : other;
    }

    public static BigDecimal min(BigDecimal one, BigDecimal other) {
        return gte(one, other) ? other : one;
    }

    public static BigDecimal valueOf(String value) {
        if (value.isEmpty()) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(value);
    }
}
