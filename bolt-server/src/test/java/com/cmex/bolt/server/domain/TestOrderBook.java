package com.cmex.bolt.server.domain;

import com.cmex.bolt.server.api.OrderSide;
import com.cmex.bolt.server.api.OrderType;
import com.cmex.bolt.server.api.PlaceOrder;
import com.cmex.bolt.server.repository.impl.SymbolRepository;
import org.junit.jupiter.api.Test;

public class TestOrderBook {
    @Test
    public void testOrderBook() {
        SymbolRepository symbolRepository = new SymbolRepository();
        Symbol btcusdt = symbolRepository.get(1).get();
        btcusdt.init();
        OrderBook orderBook = btcusdt.getOrderBook();
        PlaceOrder placeOrder1 = new PlaceOrder();
        placeOrder1.accountId.set(1);
        placeOrder1.type.set(OrderType.LIMIT);
        placeOrder1.side.set(OrderSide.ASK);
        placeOrder1.price.set(10000);
        placeOrder1.quantity.set(100000);
        Order order1= orderBook.getOrder(placeOrder1);
        System.out.println(order1);
        PlaceOrder placeOrder2 = new PlaceOrder();
        placeOrder2.accountId.set(2);
        placeOrder2.type.set(OrderType.MARKET);
        placeOrder2.side.set(OrderSide.ASK);
        placeOrder2.price.set(10001);
        placeOrder2.quantity.set(100000);
        Order order2= orderBook.getOrder(placeOrder2);
        System.out.println(order2);
        Order order3 = new Order();
        order3.setByteBuffer(order2.getByteBuffer(),0);
        System.out.println(order3.getAccountId());
        System.out.println(order3.getType());

    }
}
