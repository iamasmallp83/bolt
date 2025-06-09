package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Bolt;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCanceled extends Struct  implements Supplier<Bolt.CancelOrderResponse> {

    @Override
    public Bolt.CancelOrderResponse get() {
        return Bolt.CancelOrderResponse.newBuilder().setCode(1).build();
    }
}
