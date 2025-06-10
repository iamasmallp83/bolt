package com.cmex.bolt.repository.impl;

import com.cmex.bolt.domain.Currency;

public class CurrencyRepository extends HashMapRepository<Integer, Currency> {

    public CurrencyRepository() {
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
        holder.put(usdt.getId(), usdt);
        holder.put(btc.getId(), btc);
        holder.put(shib.getId(), shib);
        holder.put(eth.getId(), eth);
    }
}
