package com.cmex.bolt.spot.repository.impl;

import com.cmex.bolt.spot.domain.Currency;
import com.cmex.bolt.spot.domain.OrderBook;
import com.cmex.bolt.spot.domain.Symbol;

public class OrderBookRepository extends HashMapRepository<Integer, OrderBook>{

    public OrderBookRepository(){
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
        holder.put(btcusdt.getId(), new OrderBook(btcusdt));
        holder.put(shibusdt.getId(), new OrderBook(shibusdt));
    }
}
