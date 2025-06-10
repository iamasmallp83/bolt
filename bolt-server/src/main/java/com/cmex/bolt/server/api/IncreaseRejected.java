package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Envoy;
import javolution.io.Struct;

import java.util.function.Supplier;

public class IncreaseRejected extends Struct implements Supplier<Envoy.IncreaseResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    @Override
    public Envoy.IncreaseResponse get() {
        return Envoy.IncreaseResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
