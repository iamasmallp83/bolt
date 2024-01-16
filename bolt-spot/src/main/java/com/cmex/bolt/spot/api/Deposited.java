package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Deposited extends Struct implements Supplier<SpotServiceProto.DepositResponse> {
    public final Struct.Signed32 accountId = new Struct.Signed32();
    public final Struct.Signed64 value = new Struct.Signed64();
    public final Struct.Signed64 frozen = new Struct.Signed64();

    @Override
    public SpotServiceProto.DepositResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setValue(value.get())
                .setFrozen(frozen.get())
                .setAvailable(value.get() - frozen.get())
                .build();
        return SpotServiceProto.DepositResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
