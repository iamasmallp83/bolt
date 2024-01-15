package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.GenericResponseProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class WithdrawRejected extends Struct implements Supplier<GenericResponseProto.GenericResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    @Override
    public GenericResponseProto.GenericResponse get() {
        return GenericResponseProto.GenericResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
