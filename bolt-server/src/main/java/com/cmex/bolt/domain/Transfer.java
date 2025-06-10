package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import lombok.Getter;
import org.capnproto.MessageBuilder;
import org.capnproto.MessageReader;
import org.capnproto.Serialize;
import org.capnproto.StructReader;

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

    private void serialize(MessageBuilder messageBuilder, ByteBuf byteBuf) {
        try {
            ByteBufWritableChannel channel = new ByteBufWritableChannel(byteBuf);
            Serialize.write(channel, messageBuilder);
        } catch (IOException e) {
        }
    }

    public ByteBuf empty() {
        return ByteBufAllocator.DEFAULT.buffer();
    }

    public void write(Envoy.IncreaseRequest request, ByteBuf byteBuf) {
        if (request == null) {
            throw new IllegalArgumentException("IncreaseRequest cannot be null");
        }

        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.Increase.Builder increase = messageBuilder.initRoot(Nexus.Increase.factory);

        increase.setAccountId(request.getAccountId());
        //TODO
//        increase.setAmount(NumberUtils.parseDecimal(request.getAmount()));

        serialize(messageBuilder, byteBuf);
    }

    public void write(Envoy.DecreaseRequest request, ByteBuf byteBuf){
        if (request == null) {
            throw new IllegalArgumentException("DecreaseRequest cannot be null");
        }

        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.Decrease.Builder decrease = messageBuilder.initRoot(Nexus.Decrease.factory);

        decrease.setAccountId(request.getAccountId());
        //TODO
//        decrease.setAmount(NumberUtils.parseDecimal(request.getAmount()));

        serialize(messageBuilder, byteBuf);
    }

    public void write(Envoy.PlaceOrderRequest request, ByteBuf byteBuf){
        if (request == null) {
            throw new IllegalArgumentException("PlaceOrderRequest cannot be null");
        }

        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.PlaceOrder.Builder placeOrder = messageBuilder.initRoot(Nexus.PlaceOrder.factory);

        placeOrder.setSymbolId(request.getSymbolId());
        placeOrder.setAccountId(request.getAccountId());

        // 转换订单类型
        placeOrder.setType(request.getType() == Envoy.PlaceOrderRequest.Type.LIMIT ?
                Nexus.OrderType.LIMIT : Nexus.OrderType.MARKET);

        // 转换订单方向
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
        serialize(messageBuilder, byteBuf);
    }

    //
    public void write(Envoy.CancelOrderRequest request, ByteBuf byteBuf){
        if (request == null) {
            throw new IllegalArgumentException("CancelOrderRequest cannot be null");
        }
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        builder.setId(123456789);
        Nexus.CancelOrder.Builder cancelOrder = builder.getPayload().initCancelOrder();
        cancelOrder.setOrderId(request.getOrderId());
        serialize(messageBuilder, byteBuf);
    }

    public Nexus.NexusEvent.Reader to(ByteBuf byteBuf) {
        if (byteBuf == null) {
            throw new IllegalArgumentException("CancelOrderRequest cannot be null");
        }
        ByteBufReadableChannel channel = new ByteBufReadableChannel(byteBuf);
        MessageReader messageReader = null;
        try {
            messageReader = Serialize.read(channel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Nexus.NexusEvent.Reader reader = messageReader.getRoot(Nexus.NexusEvent.factory);
        return reader;
//        Nexus.Payload.Reader payloadReader = reader.getPayload();
//        switch (payloadReader.which()) {
//            case PLACE_ORDER -> {
//                return payloadReader.getPlaceOrder();
//            }
//            case CANCEL_ORDER -> {
//                return payloadReader.getCancelOrder();
//            }
//            case ORDER_CREATED -> {
//                return payloadReader.getOrderCreated();
//            }
//            case ORDER_CANCELED -> {
//                return payloadReader.getOrderCanceled();
//            }
//            case PLACE_ORDER_REJECTED -> {
//                return payloadReader.getPlaceOrderRejected();
//            }
//            case CANCEL_ORDER_REJECTED -> {
//                return payloadReader.getCancelOrderRejected();
//            }
//            case INCREASE -> {
//                return payloadReader.getIncrease();
//            }
//            case INCREASED -> {
//                return payloadReader.getIncreased();
//            }
//            case INCREASE_REJECTED -> {
//                return payloadReader.getIncreaseRejected();
//            }
//            case DECREASE -> {
//                return payloadReader.getDecrease();
//            }
//            case DECREASED -> {
//                return payloadReader.getDecreased();
//            }
//            case DECREASE_REJECTED -> {
//                return payloadReader.getDecreaseRejected();
//            }
//            case FREEZE -> {
//                return payloadReader.getFreeze();
//            }
//            case UNFREEZE -> {
//                return payloadReader.getUnfreeze();
//            }
//            case UNFROZEN -> {
//                return payloadReader.getUnfrozen();
//            }
//            case CLEARED -> {
//                return payloadReader.getCleared();
//            }
//            case _NOT_IN_SCHEMA -> {
//                throw new IllegalArgumentException("Event type _NOT_IN_SCHEMA is not supported");
//            }
//            default -> {
//                throw new IllegalArgumentException("Unsupported event type: " + payloadReader.which());
//            }
//        }
    }

    private static class ByteBufWritableChannel implements WritableByteChannel {
        private boolean open = true;
        private ByteBuf byteBuf;

        public ByteBufWritableChannel(ByteBuf byteBuf) {
            this.byteBuf = byteBuf;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!open) {
                throw new IOException("Channel is closed");
            }

            int remaining = src.remaining();
            if (remaining > 0) {
                byteBuf.writeBytes(src);  // 正确的方式：考虑position和limit
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
        private final ByteBuf byteBuf;
        private boolean open = true;

        public ByteBufReadableChannel(ByteBuf byteBuf) {
            this.byteBuf = byteBuf;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!open) {
                throw new IOException("Channel is closed");
            }

            int bytesToRead = Math.min(dst.remaining(), byteBuf.readableBytes());
            if (bytesToRead == 0) {
                return byteBuf.readableBytes() == 0 ? -1 : 0;
            }

            byteBuf.readBytes(dst);
            return bytesToRead;
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