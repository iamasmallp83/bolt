package com.cmex.bolt.util;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class NumberUtils {

    public static final Map<Integer, Double> CACHED_DOUBLE_NUMBER = new HashMap<>();
    public static final Map<Integer, Long> CACHED_LONG_NUMBER = new HashMap<>();

    static {
        for (int i = 0; i <= 18; i++) {
            CACHED_DOUBLE_NUMBER.put(i, Math.pow(10, i));
            CACHED_LONG_NUMBER.put(i, (long) Math.pow(10, i));
        }
    }

    public static double powDouble(int i) {
        if (i < 0 || i > 18) {
            throw new IllegalArgumentException("i must between 0 and 18");
        }
        return CACHED_DOUBLE_NUMBER.get(i);
    }

    public static long powLong(int i) {
        if (i < 0 || i > 18) {
            throw new IllegalArgumentException("i must between 0 and 18");
        }
        return CACHED_LONG_NUMBER.get(i);
    }

    /**
     * 将字符串形式的小数转换为长整型（假设8位小数精度）
     * 例如: "100.12345678" -> 10012345678L
     */
    public static long parseDecimal(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0L;
        }
        
        BigDecimal decimal = new BigDecimal(value.trim());
        // 假设8位小数精度
        BigDecimal scaled = decimal.multiply(BigDecimal.valueOf(powLong(8)));
        return scaled.longValue();
    }

    /**
     * 将长整型转换为字符串形式的小数（假设8位小数精度）
     * 例如: 10012345678L -> "100.12345678"
     */
    public static String formatDecimal(long value) {
        BigDecimal decimal = new BigDecimal(value);
        BigDecimal scaled = decimal.divide(BigDecimal.valueOf(powLong(8)));
        return scaled.toPlainString();
    }

}
