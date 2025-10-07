package com.cmex.bolt.repository.impl;

import com.cmex.bolt.domain.Currency;

public class CurrencyRepository extends HashMapRepository<Integer, Currency> {

    public CurrencyRepository() {
        Currency usdt = Currency.builder()
                .id(1)
                .name("USDT")
                .precision(2)
                .build();
        Currency btc = Currency.builder()
                .id(2)
                .name("BTC")
                .precision(4)
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
        Currency sol = Currency.builder()
                .id(5)
                .name("SOL")
                .precision(2)
                .build();
        Currency doge = Currency.builder()
                .id(6)
                .name("DOGE")
                .precision(2)
                .build();
        Currency aave = Currency.builder()
                .id(7)
                .name("AAVE")
                .precision(2)
                .build();
        Currency xrp = Currency.builder()
                .id(8)
                .name("XRP")
                .precision(2)
                .build();
        Currency ada = Currency.builder()
                .id(9)
                .name("ADA")
                .precision(2)
                .build();
        Currency bch = Currency.builder()
                .id(10)
                .name("BCH")
                .precision(2)
                .build();
        Currency ltc = Currency.builder()
                .id(11)
                .name("LTC")
                .precision(2)
                .build();
        holder.put(usdt.getId(), usdt);
        holder.put(btc.getId(), btc);
        holder.put(shib.getId(), shib);
        holder.put(eth.getId(), eth);
        holder.put(sol.getId(), sol);
        holder.put(doge.getId(), doge);
        holder.put(aave.getId(), aave);
        holder.put(xrp.getId(), xrp);
        holder.put(ada.getId(), ada);
        holder.put(bch.getId(), bch);
        holder.put(ltc.getId(), ltc);
    }
}
