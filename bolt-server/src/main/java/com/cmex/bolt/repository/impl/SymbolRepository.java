package com.cmex.bolt.repository.impl;

import com.cmex.bolt.domain.Currency;
import com.cmex.bolt.domain.Symbol;

public class SymbolRepository extends HashMapRepository<Integer, Symbol> {

    private static SymbolRepository INSTANCE = new SymbolRepository();

    public static SymbolRepository getInstance() {
        return INSTANCE;
    }

    private SymbolRepository() {
        CurrencyRepository currencyRepository = CurrencyRepository.getInstance();
        Currency usdt = currencyRepository.get(1).get();
        Currency btc = currencyRepository.get(2).get();
        Currency shib = currencyRepository.get(3).get();
        Currency eth = currencyRepository.get(4).get();
        Symbol btcusdt = Symbol.builder()
                .id(1)
                .name("BTCUSDT")
                .base(btc)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        btcusdt.init();

        Symbol shibusdt = Symbol.builder()
                .id(2)
                .name("SHIBUSDT")
                .base(shib)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        shibusdt.init();

        Symbol ehtusdt = Symbol.builder()
                .id(3)
                .name("ETHUSDT")
                .base(eth)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        ehtusdt.init();

        holder.put(btcusdt.getId(), btcusdt);
        holder.put(shibusdt.getId(), shibusdt);
        holder.put(ehtusdt.getId(), ehtusdt);
    }
}
