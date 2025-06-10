package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.capnproto.StructReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestTransfer {

    @Test
    public void test() throws IOException {
        Envoy.CancelOrderRequest request = Envoy.CancelOrderRequest.newBuilder()
                .setOrderId(987654321)
                .build();
        Transfer transfer = new Transfer();
        ByteBuf buf = Unpooled.buffer();
        transfer.write(request, buf);
        Nexus.NexusEvent.Reader reader = (Nexus.NexusEvent.Reader)transfer.to(buf);
        System.out.println("Cancel Order ID: " + reader.getPayload().getCancelOrder().getOrderId());
    }
}
