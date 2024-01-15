package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import com.cmex.bolt.spot.grpc.SpotServiceProto;
import com.google.protobuf.Any;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Withdrawn extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Signed32 accountId = new Signed32();
    public final Signed64 value = new Signed64();
    public final Signed64 frozen = new Signed64();

    @Override
    public GenericResponseProto.GenericResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setValue(value.get())
                .setFrozen(frozen.get()).build();
        return GenericResponseProto.GenericResponse.newBuilder().setCode(1).setData(Any.pack(balance)).build();
    }
}
