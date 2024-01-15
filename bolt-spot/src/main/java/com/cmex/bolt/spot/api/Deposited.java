package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Deposited extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Struct.Signed32 accountId = new Struct.Signed32();
    public final Struct.Signed16 currencyId = new Struct.Signed16();
    public final Struct.Signed64 balance = new Struct.Signed64();
    public final Struct.Signed64 frozen = new Struct.Signed64();

    @Override
    public GenericResponseProto.GenericResponse get() {
        return GenericResponseProto.GenericResponse.newBuilder().setCode(1).build();
    }
}
