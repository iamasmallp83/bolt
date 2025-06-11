package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import org.capnproto.ArrayOutputStream;
import org.capnproto.MessageBuilder;
import org.capnproto.Serialize;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;


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
        System.out.println(reader.getPayload().which());
    }

    @Test
    public void testOriginal() throws IOException {
        Currency currency = Currency.builder()
                .name("USDT")
                .precision(4)
                .build();
        Envoy.IncreaseRequest request = Envoy.IncreaseRequest.newBuilder()
                .setAccountId(5)
                .setAmount("100")
                .setCurrencyId(2)
                .build();
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder event = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Payload.Builder payload = event.getPayload();
        Nexus.Increase.Builder increase = payload.initIncrease();
        increase.setAccountId(5);
        increase.setAmount(100000);
        increase.setCurrencyId(2);
        ArrayOutputStream outputStream = new ArrayOutputStream(ByteBuffer.allocate(512));
        Serialize.write(outputStream, messageBuilder);
        ByteBuffer write = outputStream.getWriteBuffer();
        write.flip();
        byte[] bytes = new byte[write.remaining()];
        write.get(bytes);
        System.out.println("array length " + bytes.length);
        for (byte aByte : bytes) {
            System.out.print(aByte + " ,");
        }
//        ArrayInputStream inputStream = new ArrayInputStream(write);
//        MessageReader messageReader = Serialize.read(inputStream);
//        Nexus.NexusEvent.Reader reader = messageReader.getRoot(Nexus.NexusEvent.factory);
//        Nexus.Increase.Reader increaseReader = reader.getPayload().getIncrease();
//        System.out.println(increaseReader.getAccountId());
//        System.out.println(increaseReader.getAmount());
//        System.out.println(increaseReader.getCurrencyId());
    }
}
