package com.cmex.bolt.server.api;

import com.cmex.bolt.server.grpc.Envoy;
import javolution.io.Struct;

import java.util.function.Supplier;

public class Decreased extends Struct implements Supplier<Envoy.DecreaseResponse> {
    public final UTF8String currency = new UTF8String(8);
    public final UTF8String value = new UTF8String(18);
    public final UTF8String frozen = new UTF8String(18);
    public final UTF8String available = new UTF8String(18);

    @Override
    public Envoy.DecreaseResponse get() {
        Envoy.Balance balance = Envoy.Balance.newBuilder()
                .setCurrency(currency.get())
                .setValue(value.get())
                .setFrozen(frozen.get())
                .setAvailable(available.get())
                .build();
        return Envoy.DecreaseResponse.newBuilder().setCode(1).setData(balance).build();
    }
}
