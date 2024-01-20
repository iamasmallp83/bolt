package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCanceled extends Struct  implements Supplier<SpotServiceProto.CancelOrderResponse> {

    @Override
    public SpotServiceProto.CancelOrderResponse get() {
        return SpotServiceProto.CancelOrderResponse.newBuilder().setCode(1).build();
    }
}
