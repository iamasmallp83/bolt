package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class CancelOrderRejected extends Struct implements Supplier<SpotServiceProto.CancelOrderResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    public SpotServiceProto.CancelOrderResponse get() {
        return SpotServiceProto.CancelOrderResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
