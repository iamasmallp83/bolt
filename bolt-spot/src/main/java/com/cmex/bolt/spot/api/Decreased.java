package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Decreased extends Struct implements Supplier<SpotServiceProto.DecreaseResponse> {
    public final UTF8String currency = new UTF8String(8);
    public final UTF8String value = new UTF8String(18);
    public final UTF8String frozen = new UTF8String(18);
    public final UTF8String available = new UTF8String(18);

    @Override
    public SpotServiceProto.DecreaseResponse get() {
        SpotServiceProto.Balance balance = SpotServiceProto.Balance.newBuilder()
                .setCurrency(currency.get())
                .setValue(value.get())
                .setFrozen(frozen.get())
                .setAvailable(available.get())
                .build();
        return SpotServiceProto.DecreaseResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
