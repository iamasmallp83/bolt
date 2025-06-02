package com.cmex.bolt.spot.repository.impl;

import com.cmex.bolt.spot.domain.Currency;
import com.cmex.bolt.spot.domain.Symbol;

public class SymbolRepository extends HashMapRepository<Integer, Symbol> {

    public SymbolRepository() {
        Currency usdt = Currency.builder()
                .id(1)
                .name("USDT")
                .precision(4)
                .build();
        Currency btc = Currency.builder()
                .id(2)
                .name("BTC")
                .precision(6)
                .build();
        Currency shib = Currency.builder()
                .id(3)
                .name("SHIB")
                .precision(0)
                .build();
        Currency eth = Currency.builder()
                .id(4)
                .name("ETH")
                .precision(4)
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
        Symbol ehtusdt = Symbol.builder()
                .id(3)
                .name("ETHUSDT")
                .base(eth)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        holder.put(btcusdt.getId(), btcusdt);
        holder.put(shibusdt.getId(), shibusdt);
        holder.put(ehtusdt.getId(), ehtusdt);
    }
}
