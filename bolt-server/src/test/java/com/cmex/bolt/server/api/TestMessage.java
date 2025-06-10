package com.cmex.bolt.server.api;

import com.cmex.bolt.server.schema.Nexus;
import org.capnproto.MessageBuilder;
import org.junit.jupiter.api.Test;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestMessage {

    @Test
    public void testSize() throws IOException {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.CancelOrder.Builder builder =  messageBuilder.initRoot(Nexus.CancelOrder.factory);
        builder.setOrderId(1000);
        org.capnproto.SerializePacked.writeToUnbuffered(
                (new FileOutputStream(FileDescriptor.out)).getChannel(),
                messageBuilder);
    }
}
