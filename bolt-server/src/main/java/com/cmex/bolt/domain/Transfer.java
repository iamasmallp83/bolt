package com.cmex.bolt.domain;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
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

    public void serialize(MessageBuilder messageBuilder, ByteBuf buffer) {
        try {
            ByteBufWritableChannel channel = new ByteBufWritableChannel(buffer);
            Serialize.write(channel, messageBuilder);
        } catch (IOException e) {
        }
    }

    public ByteBuf empty() {
        return ByteBufAllocator.DEFAULT.buffer();
    }

    public void writeIncreaseRequest(Envoy.IncreaseRequest request, Currency currency, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Increase.Builder increase = builder.getPayload().initIncrease();

        increase.setAccountId(request.getAccountId());
        increase.setCurrencyId(currency.getId());
        increase.setAmount(currency.parse(request.getAmount()));

        serialize(messageBuilder, buffer);
    }

    public void writeDecreaseRequest(Envoy.DecreaseRequest request, Currency currency, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Decrease.Builder decrease = builder.getPayload().initDecrease();

        decrease.setAccountId(request.getAccountId());
        decrease.setCurrencyId(currency.getId());
        decrease.setAmount(currency.parse(request.getAmount()));

        serialize(messageBuilder, buffer);
    }

    public void writePlaceOrderRequest(Envoy.PlaceOrderRequest request, Symbol symbol, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.PlaceOrder.Builder placeOrder = builder.getPayload().initPlaceOrder();
        placeOrder.setSymbolId(request.getSymbolId());
        placeOrder.setAccountId(request.getAccountId());
        placeOrder.setType(request.getType() == Envoy.Type.LIMIT ?
                Nexus.OrderType.LIMIT : Nexus.OrderType.MARKET);
        placeOrder.setSide(request.getSide() == Envoy.Side.BID ?
                Nexus.OrderSide.BID : Nexus.OrderSide.ASK);
        long price = symbol.formatPrice(request.getPrice());
        placeOrder.setPrice(price);
        long quantity = symbol.formatQuantity(request.getQuantity());
        placeOrder.setQuantity(quantity);
        long volume = symbol.formatPrice(request.getVolume());
        placeOrder.setVolume(volume);
        placeOrder.setFrozen(calculateFrozen(symbol, request.getSide(), volume, price, quantity, request.getTakerRate()));
        placeOrder.setTakerRate(request.getTakerRate());
        placeOrder.setMakerRate(request.getMakerRate());
        serialize(messageBuilder, buffer);
    }

    private long calculateFrozen(Symbol symbol, Envoy.Side side, long volume,
                                 long price, long quantity, int takerRate) {
        long amount;
        if (side == Envoy.Side.BID) {
            if (volume > 0) {
                amount = volume;
            } else {
                amount = symbol.getVolume(price, quantity);
            }
            if (symbol.isQuoteSettlement()) {
                amount += Rate.getRate(amount, takerRate);
            }
        } else {
            amount = quantity;
        }
        return amount;
    }

    public void writeCancelOrderRequest(Envoy.CancelOrderRequest request, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.CancelOrder.Builder cancelOrder = builder.getPayload().initCancelOrder();
        cancelOrder.setOrderId(request.getOrderId());
        serialize(messageBuilder, buffer);
    }

    public void writeFailed(Nexus.EventType failed, Nexus.RejectionReason reason, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        switch (failed) {
            case DECREASE_REJECTED -> {
                Nexus.DecreaseRejected.Builder decreaseRejected = builder.getPayload().initDecreaseRejected();
                decreaseRejected.setReason(reason);
            }
            case PLACE_ORDER_REJECTED -> {
                Nexus.PlaceOrderRejected.Builder placeOrderRejected = builder.getPayload().initPlaceOrderRejected();
                placeOrderRejected.setReason(reason);
            }
            case CANCEL_ORDER_REJECTED -> {
                Nexus.CancelOrderRejected.Builder cancelOrderRejected = builder.getPayload().initCancelOrderRejected();
                cancelOrderRejected.setReason(reason);
            }
        }
        serialize(messageBuilder, buffer);
    }

    public void writeBalance(Balance balance, Nexus.EventType type, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        switch (type) {
            case INCREASED -> {
                Nexus.Increased.Builder increased = builder.getPayload().initIncreased();
                increased.setCurrencyId(balance.getCurrency().getId());
                increased.setAmount(balance.getValue());
                increased.setAvailable(balance.getValue());
                increased.setFrozen(balance.getFrozen());
                serialize(messageBuilder, buffer);
            }
            case DECREASED -> {
                Nexus.Decreased.Builder decreased = builder.getPayload().initDecreased();
                decreased.setCurrencyId(balance.getCurrency().getId());
                decreased.setAmount(balance.getValue());
                decreased.setAvailable(balance.getValue());
                decreased.setFrozen(balance.getFrozen());
                serialize(messageBuilder, buffer);
            }
            default -> {
                //TODO
            }
        }
        serialize(messageBuilder, buffer);
    }

    public void writeOrder(Order order, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.OrderCreated.Builder orderCreated = builder.getPayload().initOrderCreated();
        orderCreated.setOrderId(order.getId());
        serialize(messageBuilder, buffer);
    }


    public void writePlaceOrder(Nexus.PlaceOrder.Reader reader, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.PlaceOrder.Builder placeOrder = builder.getPayload().initPlaceOrder();
        placeOrder.setSymbolId(reader.getSymbolId());
        placeOrder.setAccountId(reader.getAccountId());
        placeOrder.setType(reader.getType());
        placeOrder.setSide(reader.getSide());
        placeOrder.setPrice(reader.getPrice());
        placeOrder.setQuantity(reader.getQuantity());
        placeOrder.setVolume(reader.getVolume());
        placeOrder.setFrozen(reader.getFrozen());
        placeOrder.setTakerRate(reader.getTakerRate());
        placeOrder.setMakerRate(reader.getMakerRate());
        serialize(messageBuilder, buffer);
    }

    public void writeCancelOrder(Nexus.CancelOrder.Reader cancelOrder, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.OrderCanceled.Builder orderCanceled = builder.getPayload().initOrderCanceled();
        orderCanceled.setOrderId(cancelOrder.getOrderId());
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

    public Object to(CurrencyRepository repository, ByteBuf buffer) {
        Nexus.NexusEvent.Reader reader = from(buffer);
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
            case INCREASED -> {
                Nexus.Increased.Reader increased = payload.getIncreased();
                Currency currency = repository.get(increased.getCurrencyId()).get();
                return Envoy.IncreaseResponse.newBuilder().setCode(1).setData(
                        Envoy.Balance.newBuilder()
                                .setCurrency(currency.getName())
                                .setValue(currency.format(increased.getAmount()))
                                .setAvailable(currency.format(increased.getAvailable()))
                                .setFrozen(currency.format(increased.getFrozen())).build()
                ).build();
            }
            case DECREASED -> {
                Nexus.Decreased.Reader decreased = payload.getDecreased();
                Currency currency = repository.get(decreased.getCurrencyId()).get();
                return Envoy.IncreaseResponse.newBuilder().setCode(1).setData(
                        Envoy.Balance.newBuilder()
                                .setCurrency(currency.getName())
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
            case ORDER_CREATED -> {
                Nexus.OrderCreated.Reader orderCreated = payload.getOrderCreated();
                return Envoy.PlaceOrderResponse.newBuilder()
                        .setCode(1)
                        .setData(Envoy.Order.newBuilder().setId(orderCreated.getOrderId()).build()).build();
            }
            case PLACE_ORDER_REJECTED -> {
                Nexus.PlaceOrderRejected.Reader rejected = payload.getPlaceOrderRejected();
                return Envoy.PlaceOrderResponse.newBuilder()
                        .setCode(rejected.getReason().ordinal())
                        .setMessage(rejected.getReason().name()).build();
            }
            case ORDER_CANCELED -> {
                return Envoy.CancelOrderResponse.newBuilder()
                        .setCode(1)
                        .build();
            }
            case CANCEL_ORDER_REJECTED -> {
                Nexus.CancelOrderRejected.Reader rejected = payload.getCancelOrderRejected();
                return Envoy.CancelOrderResponse.newBuilder()
                        .setCode(rejected.getReason().ordinal())
                        .setMessage(rejected.getReason().name()).build();
            }
            default -> {
                throw new IllegalArgumentException("Unknown event type: " + payload.which());
            }
        }
    }

    public void writeUnfreeze(Symbol symbol, Order order, ByteBuf buffer) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Unfreeze.Builder unfreeze = builder.getPayload().initUnfreeze();
        unfreeze.setAccountId(order.getAccountId());
        unfreeze.setCurrencyId(symbol.getPayCurrency(order.getSide()).getId());
        unfreeze.setAmount(order.calculateUnfreeze());
        serialize(messageBuilder, buffer);
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
            dst.limit(dst.position() + bytesToRead);
            buffer.readBytes(dst);
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