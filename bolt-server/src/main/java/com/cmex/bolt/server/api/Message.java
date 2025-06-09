package com.cmex.bolt.server.api;

import com.lmax.disruptor.EventFactory;
import javolution.io.Struct;

import java.nio.ByteBuffer;

public class Message extends Struct {
    public final Enum32<EventType> type = new Enum32<>(EventType.values());
    public final Signed64 id = new Struct.Signed64();
    public final SpotEvent payload = inner(new SpotEvent());

    @Override
    public String toString() {
        return "Message [type=" + type.get() + "]";
    }

    public final static EventFactory<Message> FACTORY = () -> {
        Message message = new Message();
        message.setByteBuffer(ByteBuffer.allocate(message.size()), 0);
        return message;
    };

}
