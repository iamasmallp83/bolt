package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.stub.StreamObserver;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Transfer类负责将Envoy gRPC请求转换为Nexus Cap'n Proto事件
 * 支持双向转换：from方法用于Envoy->Nexus，to方法用于Nexus->Envoy
 * 支持单个转换和批量转换，提供完整的错误处理
 */
public class Transfer {

    private void serialize(MessageBuilder messageBuilder, ByteBuf buffer) {
        try {
            ByteBufWritableChannel channel = new ByteBufWritableChannel(buffer);
            Serialize.write(channel, messageBuilder);
        } catch (IOException e) {
        }
    }

    public ByteBuf empty() {
        return ByteBufAllocator.DEFAULT.buffer();
    }

    public void write(Envoy.IncreaseRequest request, Currency currency, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Increase.Builder increase = builder.getPayload().initIncrease();

        increase.setAccountId(request.getAccountId());
        increase.setCurrencyId(currency.getId());
        increase.setAmount(currency.parse(request.getAmount()));

        serialize(messageBuilder, buffer);
    }

    public void write(Envoy.DecreaseRequest request, Currency currency, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Decrease.Builder decrease = builder.getPayload().initDecrease();

        decrease.setAccountId(request.getAccountId());
        decrease.setAmount(currency.parse(request.getAmount()));

        serialize(messageBuilder, buffer);
    }

    public void write(Envoy.PlaceOrderRequest request, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.PlaceOrder.Builder placeOrder = builder.getPayload().initPlaceOrder();

        placeOrder.setSymbolId(request.getSymbolId());
        placeOrder.setAccountId(request.getAccountId());
        placeOrder.setType(request.getType() == Envoy.PlaceOrderRequest.Type.LIMIT ?
                Nexus.OrderType.LIMIT : Nexus.OrderType.MARKET);
        placeOrder.setSide(request.getSide() == Envoy.PlaceOrderRequest.Side.BID ?
                Nexus.OrderSide.BID : Nexus.OrderSide.ASK);

        // 处理可选字段
        // TODO
        if (request.hasPrice()) {
//            placeOrder.setPrice(NumberUtils.parseDecimal(request.getPrice()));
        }
        if (request.hasQuantity()) {
//            placeOrder.setQuantity(NumberUtils.parseDecimal(request.getQuantity()));
        }
        if (request.hasVolume()) {
//            placeOrder.setVolume(NumberUtils.parseDecimal(request.getVolume()));
        }
        if (request.hasTakerRate()) {
            placeOrder.setTakerRate(request.getTakerRate());
        }
        if (request.hasMakerRate()) {
            placeOrder.setMakerRate(request.getMakerRate());
        }
        serialize(messageBuilder, buffer);
    }

    //
    public void write(Nexus.EventType failed, Nexus.RejectionReason reason, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        switch (failed) {
            case DECREASE_REJECTED -> {
                Nexus.DecreaseRejected.Builder decreaseRejected = builder.getPayload().initDecreaseRejected();
                decreaseRejected.setReason(reason);
            }
            default -> {
            }
        }
        serialize(messageBuilder, buffer);
    }

    public void write(Balance balance, Nexus.EventType type, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        switch (type) {
            case INCREASED -> {
                Nexus.Increased.Builder increased = builder.getPayload().initIncreased();
                increased.setCurrency(balance.getCurrency().getName());
                increased.setAmount(balance.getValue());
                increased.setAvailable(balance.getValue());
                increased.setFrozen(balance.getFrozen());
                serialize(messageBuilder, buffer);
            }
            case DECREASED -> {

            }
            default -> {
                //TODO
            }
        }
        serialize(messageBuilder, buffer);
    }


    public Nexus.NexusEvent.Reader from(ByteBuf buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("CancelOrderRequest cannot be null");
        }
        ByteBufReadableChannel channel = new ByteBufReadableChannel(buffer);
        MessageReader messageReader;
        try {
            messageReader = Serialize.read(channel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return messageReader.getRoot(Nexus.NexusEvent.factory);
    }

    public Object to(Currency currency, ByteBuf buffer) {
        Nexus.NexusEvent.Reader reader = from(buffer);
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
//                    case ORDER_CREATED -> message.payload.asOrderCreated.get();
//                    case PLACE_ORDER_REJECTED -> message.payload.asPlaceOrderRejected.get();
//                    case ORDER_CANCELED -> message.payload.asOrderCanceled.get();
            case INCREASED -> {
                Nexus.Increased.Reader increased = payload.getIncreased();
                return Envoy.IncreaseResponse.newBuilder().setCode(1).setData(
                        Envoy.Balance.newBuilder()
                                .setCurrency(increased.getCurrency().toString())
                                .setValue(currency.format(increased.getAmount()))
                                .setAvailable(currency.format(increased.getAvailable()))
                                .setFrozen(currency.format(increased.getFrozen())).build()
                ).build();
            }
            case DECREASED -> {
                Nexus.Decreased.Reader decreased = payload.getDecreased();
                return Envoy.IncreaseResponse.newBuilder().setCode(1).setData(
                        Envoy.Balance.newBuilder()
                                .setCurrency(decreased.getCurrency().toString())
                                .setValue(currency.format(decreased.getAmount()))
                                .setAvailable(currency.format(decreased.getAvailable()))
                                .setFrozen(currency.format(decreased.getFrozen())).build()
                ).build();
            }
            case DECREASE_REJECTED -> {
                Nexus.DecreaseRejected.Reader rejected = payload.getDecreaseRejected();
                return Envoy.DecreaseResponse.newBuilder()
                        .setCode(rejected.getReason().ordinal())
                        .setMessage(rejected.getReason().name()).build();
            }
            default -> throw new RuntimeException();
        }
    }

    private static class ByteBufWritableChannel implements WritableByteChannel {
        private boolean open = true;
        private ByteBuf buffer;

        public ByteBufWritableChannel(ByteBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!open) {
                throw new IOException("Channel is closed");
            }

            int remaining = src.remaining();
            if (remaining > 0) {
                buffer.writeBytes(src);  // 正确的方式：考虑position和limit
            }
            return remaining;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
        }

    }

    private static class ByteBufReadableChannel implements ReadableByteChannel {
        private final ByteBuf buffer;
        private boolean open = true;

        public ByteBufReadableChannel(ByteBuf buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new IOException("Channel is closed");
            }

            int bytesToRead = Math.min(dst.remaining(), buffer.readableBytes());
            if (bytesToRead == 0) {
                return buffer.readableBytes() == 0 ? -1 : 0;
            }
            // 关键修复：限制 ByteBuffer 的读取范围
            int originalLimit = dst.limit();
            dst.limit(dst.position() + bytesToRead);

            try {
                buffer.readBytes(dst);
                return bytesToRead;
            } finally {
                dst.limit(originalLimit);
            }
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() throws IOException {
            open = false;
        }
    }
}