package com.cmex.bolt.spot.repository.impl;

import com.cmex.bolt.spot.domain.Currency;
import com.cmex.bolt.spot.domain.Symbol;

public class SymbolRepository extends HashMapRepository<Integer, Symbol> {

    public SymbolRepository() {
        Currency usdt = Currency.builder()
                .id(1)
                .name("USDT")
                .precision(6)
                .build();
        Currency btc = Currency.builder()
                .id(2)
                .name("BTC")
                .precision(8)
                .build();
        Currency shib = Currency.builder()
                .id(3)
                .name("SHIB")
                .precision(0)
                .build();
        Symbol btcusdt = Symbol.builder()
                .id(1)
                .name("BTCUSDT")
                .base(btc)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        Symbol shibusdt = Symbol.builder()
                .id(2)
                .name("SHIBUSDT")
                .base(shib)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        holder.put(btcusdt.getId(), btcusdt);
        holder.put(shibusdt.getId(), shibusdt);
    }
}
