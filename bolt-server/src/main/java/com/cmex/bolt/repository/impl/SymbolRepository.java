package com.cmex.bolt.repository.impl;

import com.cmex.bolt.domain.Currency;
import com.cmex.bolt.domain.Symbol;

public class SymbolRepository extends HashMapRepository<Integer, Symbol> {

    public SymbolRepository(CurrencyRepository currencyRepository) {
        Currency usdt = currencyRepository.get(1).get();
        Currency btc = currencyRepository.get(2).get();
        Currency shib = currencyRepository.get(3).get();
        Currency eth = currencyRepository.get(4).get();
        Currency sol = currencyRepository.get(5).get();
        Currency doge = currencyRepository.get(6).get();
        Currency aave = currencyRepository.get(7).get();
        Currency xrp = currencyRepository.get(8).get();
        Currency ada = currencyRepository.get(9).get();
        Currency bch = currencyRepository.get(10).get();
        Currency ltc = currencyRepository.get(11).get();
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

        Symbol solusdt = Symbol.builder()
                .id(4)
                .name("SOLUSDT")
                .base(sol)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        solusdt.init();

        Symbol dogeusdt = Symbol.builder()
                .id(5)
                .name("DOGEUSDT")
                .base(doge)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        dogeusdt.init();

        Symbol aaveusdt = Symbol.builder()
                .id(6)
                .name("AAVEUSDT")
                .base(aave)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        aaveusdt.init();

        Symbol xrpusdt = Symbol.builder()
                .id(7)
                .name("XRPUSDT")
                .base(xrp)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        xrpusdt.init();

        Symbol adausdt = Symbol.builder()
                .id(8)
                .name("ADAUSDT")
                .base(ada)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        adausdt.init();

        Symbol bchusdt = Symbol.builder()
                .id(9)
                .name("BCHUSDT")
                .base(bch)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        bchusdt.init();

        Symbol ltcusdt = Symbol.builder()
                .id(10)
                .name("LTCUSDT")
                .base(ltc)
                .quote(usdt)
                .quoteSettlement(true)
                .build();
        ltcusdt.init();

        holder.put(btcusdt.getId(), btcusdt);
        holder.put(shibusdt.getId(), shibusdt);
        holder.put(ehtusdt.getId(), ehtusdt);
        holder.put(solusdt.getId(), solusdt);
        holder.put(dogeusdt.getId(), dogeusdt);
        holder.put(aaveusdt.getId(), aaveusdt);
        holder.put(xrpusdt.getId(), xrpusdt);
        holder.put(adausdt.getId(), adausdt);
        holder.put(bchusdt.getId(), bchusdt);
        holder.put(ltcusdt.getId(), ltcusdt);
    }
}
