package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;


public class TestTransfer {

    @Test
    public void test() throws IOException {
        Currency currency = Currency.builder()
                .id(2)
                .name("USDT")
                .precision(4)
                .build();
        Envoy.IncreaseRequest request = Envoy.IncreaseRequest.newBuilder()
                .setAccountId(5)
                .setAmount("10")
                .setCurrencyId(2)
                .build();
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 512);
        ByteBuf buffer = wrapper.getBuffer();
        Transfer transfer = new Transfer();
        transfer.write(request, currency, buffer);
        Nexus.NexusEvent.Reader reader = transfer.from(buffer);
        Assertions.assertEquals(reader.getPayload().which(), Nexus.Payload.Which.INCREASE);
    }

}
