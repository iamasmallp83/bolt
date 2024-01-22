package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Decreased extends Struct implements Supplier<SpotServiceProto.DecreaseResponse> {
    public final Struct.UTF8String currency = new Struct.UTF8String(16);
    public final Struct.Signed64 value = new Struct.Signed64();
    public final Struct.Signed64 frozen = new Struct.Signed64();

    @Override
    public SpotServiceProto.DecreaseResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setCurrency(currency.get())
                .setValue(String.valueOf(value.get()))
                .setFrozen(String.valueOf(frozen.get()))
                .setAvailable(String.valueOf(value.get() - frozen.get()))
                .build();
        return SpotServiceProto.DecreaseResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
