package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class OrderCreated extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Signed64 orderId = new Signed64();

    @Override
    public GenericResponseProto.GenericResponse get() {
        return GenericResponseProto.GenericResponse.newBuilder()
                .setCode(1).build();
    }
}
