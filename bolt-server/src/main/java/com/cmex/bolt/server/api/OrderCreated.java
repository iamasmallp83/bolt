package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Bolt;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCreated extends Struct implements Supplier<Bolt.PlaceOrderResponse> {
    public final Signed64 orderId = new Signed64();

    @Override
    public Bolt.PlaceOrderResponse get() {
        Bolt.Order order = Bolt.Order.newBuilder().setId(orderId.get()).build();
        return Bolt.PlaceOrderResponse.newBuilder()
                .setCode(1)
                .setData(order)
                .build();
    }
}
