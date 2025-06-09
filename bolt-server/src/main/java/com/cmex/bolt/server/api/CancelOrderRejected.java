package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Bolt;
import javolution.io.Struct;

import java.util.function.Supplier;

public class CancelOrderRejected extends Struct implements Supplier<Bolt.CancelOrderResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    public Bolt.CancelOrderResponse get() {
        return Bolt.CancelOrderResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
