package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Withdrawn extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Signed32 accountId = new Signed32();
    public final Signed16 currencyId = new Signed16();
    public final Signed64 amount = new Signed64();

    @Override
    public GenericResponseProto.GenericResponse get() {
        return GenericResponseProto.GenericResponse.newBuilder().setCode(1).build();
    }
}
