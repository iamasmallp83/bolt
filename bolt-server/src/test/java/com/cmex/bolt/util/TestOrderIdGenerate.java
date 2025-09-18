package com.cmex.bolt.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOrderIdGenerate {
    @Test
    public void test() {
        OrderIdGenerator orderIdGenerator = new OrderIdGenerator();
        int symbolId = 1;
        long id = orderIdGenerator.nextId(symbolId);
        Assertions.assertEquals(symbolId, OrderIdGenerator.getSymbolId(id));
    }
}
