package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Envoy;
import javolution.io.Struct;

import java.util.function.Supplier;

public class PlaceOrderRejected extends Struct implements Supplier<Envoy.PlaceOrderResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    public Envoy.PlaceOrderResponse get() {
        return Envoy.PlaceOrderResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
