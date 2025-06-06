package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.repository.impl.SymbolRepository;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class TestOrder {

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void test() {
        // 获取Symbol对象
        SymbolRepository symbolRepository = new SymbolRepository();
        Symbol btcusdt = symbolRepository.get(1).get();
        
        // 使用新的javolution Order对象
        Order order = new Order();
        order.setByteBuffer(ByteBuffer.allocateDirect(order.size()), 0);
        order.init(btcusdt, 1, 1, Order.OrderType.LIMIT, Order.OrderSide.BID, 
                  100000, 100, 10000000, 10000000, 100, 50);
        
        System.out.println("Order volume: " + order.getVolume());
        System.out.println("Order price: " + order.getPrice());
        System.out.println("Order quantity: " + order.getQuantity());
        System.out.println("Symbol: " + btcusdt.getName());
    }
}
