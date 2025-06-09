package com.cmex.bolt.server.api;


import com.cmex.bolt.server.grpc.Bolt;
import javolution.io.Struct;

import java.util.function.Supplier;

public class DecreaseRejected extends Struct implements Supplier<Bolt.DecreaseResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    @Override
    public Bolt.DecreaseResponse get() {
        return Bolt.DecreaseResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
