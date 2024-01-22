package com.cmex.bolt.spot.api;

import com.cmex.bolt.spot.grpc.SpotServiceProto;
import javolution.io.Struct;

import java.util.function.Supplier;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.IncreaseResponse;

public class IncreaseRejected extends Struct implements Supplier<SpotServiceProto.IncreaseResponse> {
    public final Enum32<RejectionReason> reason = new Enum32<RejectionReason>(RejectionReason.values());

    @Override
    public IncreaseResponse get() {
        return SpotServiceProto.IncreaseResponse.newBuilder()
                .setCode(reason.get().getCode()).setMessage(reason.get().name()).build();
    }
}
