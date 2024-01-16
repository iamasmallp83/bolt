package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class PlaceOrderRejected extends Struct implements Supplier<SpotServiceProto.PlaceOrderResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    public SpotServiceProto.PlaceOrderResponse get() {
        return SpotServiceProto.PlaceOrderResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
