package com.cmex.bolt.api;

import com.cmex.bolt.Nexus;
import org.capnproto.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestMessage {

    @Test
    public void testSize() throws IOException {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder event = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        event.setId(1);
        event.setType(Nexus.EventType.CANCEL_ORDER);
        Nexus.Payload.Builder payload = event.getPayload();
        Nexus.CancelOrder.Builder cancel = payload.initCancelOrder();
        cancel.setOrderId(666666);
        ArrayOutputStream outputStream = new ArrayOutputStream(ByteBuffer.allocate(512));
        Serialize.write(outputStream, messageBuilder);
        ByteBuffer write =  outputStream.getWriteBuffer();
        write.flip();
        ArrayInputStream inputStream = new ArrayInputStream(write);
        MessageReader messageReader = Serialize.read(inputStream);
        Nexus.NexusEvent.Reader reader = messageReader.getRoot(Nexus.NexusEvent.factory);
        System.out.println(reader.getPayload().getCancelOrder().getOrderId());
    }
}
