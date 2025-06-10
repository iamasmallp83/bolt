package com.cmex.bolt.domain;

import com.cmex.bolt.repository.impl.SymbolRepository;
import org.junit.jupiter.api.Test;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        SymbolRepository symbolRepository = new SymbolRepository();
        Symbol btcusdt = symbolRepository.get(1).get();
        btcusdt.init();
    }
}
