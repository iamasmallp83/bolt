package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

import static com.cmex.bolt.spot.grpc.SpotServiceProto.Order;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.PlaceOrderResponse;

public class OrderCreated extends Struct implements Supplier<PlaceOrderResponse> {
    public final Signed64 orderId = new Signed64();

    @Override
    public SpotServiceProto.PlaceOrderResponse get() {
        Order order = Order.newBuilder().setId(orderId.get()).build();
        return SpotServiceProto.PlaceOrderResponse.newBuilder()
                .setCode(1)
                .setData(order)
                .build();
    }
}
