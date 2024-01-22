package com.cmex.bolt.spot.repository.impl;

import com.cmex.bolt.spot.domain.Currency;
import org.checkerframework.checker.units.qual.C;

public class CurrencyRepository extends HashMapRepository<Integer, Currency> {

    public CurrencyRepository() {
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
        this.getOrCreate(usdt.getId(), usdt);
        this.getOrCreate(btc.getId(), btc);
        this.getOrCreate(shib.getId(), shib);
    }
}
