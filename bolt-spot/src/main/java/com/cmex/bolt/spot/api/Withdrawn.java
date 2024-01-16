package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Withdrawn extends Struct implements Supplier<SpotServiceProto.WithdrawResponse> {
    public final Signed32 accountId = new Signed32();
    public final Signed64 value = new Signed64();
    public final Signed64 frozen = new Signed64();

    @Override
    public SpotServiceProto.WithdrawResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setValue(value.get())
                .setFrozen(frozen.get())
                .setAvailable(value.get() - frozen.get()).build();
        return SpotServiceProto.WithdrawResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
