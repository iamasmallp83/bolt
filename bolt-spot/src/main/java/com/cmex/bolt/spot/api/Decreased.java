package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Decreased extends Struct implements Supplier<SpotServiceProto.DecreaseResponse> {
    public final Signed32 accountId = new Signed32();
    public final Signed64 value = new Signed64();
    public final Signed64 frozen = new Signed64();

    @Override
    public SpotServiceProto.DecreaseResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setValue(value.get())
                .setFrozen(frozen.get())
                .setAvailable(value.get() - frozen.get()).build();
        return SpotServiceProto.DecreaseResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
