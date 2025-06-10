package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Envoy;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCanceled extends Struct  implements Supplier<Envoy.CancelOrderResponse> {

    @Override
    public Envoy.CancelOrderResponse get() {
        return Envoy.CancelOrderResponse.newBuilder().setCode(1).build();
    }
}
