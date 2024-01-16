package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class WithdrawRejected extends Struct implements Supplier<SpotServiceProto.WithdrawResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    @Override
    public SpotServiceProto.WithdrawResponse get() {
        return SpotServiceProto.WithdrawResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
