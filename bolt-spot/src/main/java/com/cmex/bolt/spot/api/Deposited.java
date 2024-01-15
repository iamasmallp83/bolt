package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.google.protobuf.Any;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Deposited extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Struct.Signed32 accountId = new Struct.Signed32();
    public final Struct.Signed64 value = new Struct.Signed64();
    public final Struct.Signed64 frozen = new Struct.Signed64();

    @Override
    public GenericResponseProto.GenericResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setValue(value.get())
                .setFrozen(frozen.get()).build();
        return GenericResponseProto.GenericResponse.newBuilder().setCode(1).setData(Any.pack(balance)).build();
    }
}
