package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Envoy;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCreated extends Struct implements Supplier<Envoy.PlaceOrderResponse> {
    public final Signed64 orderId = new Signed64();

    @Override
    public Envoy.PlaceOrderResponse get() {
        Envoy.Order order = Envoy.Order.newBuilder().setId(orderId.get()).build();
        return Envoy.PlaceOrderResponse.newBuilder()
                .setCode(1)
                .setData(order)
                .build();
    }
}
