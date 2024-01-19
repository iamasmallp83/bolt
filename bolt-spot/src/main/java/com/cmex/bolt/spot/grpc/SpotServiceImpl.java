package com.cmex.bolt.spot.grpc;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.service.MatchDispatcher;
import com.cmex.bolt.spot.service.SequencerDispatcher;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.cmex.bolt.spot.grpc.SpotServiceGrpc.SpotServiceImplBase;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.*;

public class SpotServiceImpl extends SpotServiceImplBase {

    private final RingBuffer<Message> sequencerRingBuffer;

    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers = new ConcurrentHashMap<>();

    public SpotServiceImpl() {
        int bufferSize = 1024;
        Disruptor<Message> sequencerDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> matchDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> responseDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        SequencerDispatcher sequencerDispatcher = getSequencerDispatcher();
        List<MatchDispatcher> matchDispatchers = getOrderDispatchers();
        sequencerDisruptor.handleEventsWith(sequencerDispatcher);
        matchDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        sequencerRingBuffer = sequencerDisruptor.start();
        RingBuffer<Message> matchRingBuffer = matchDisruptor.start();
        RingBuffer<Message> responseRingBuffer = responseDisruptor.start();
        sequencerDispatcher.getAccountService().setMatchRingBuffer(matchRingBuffer);
        sequencerDispatcher.getAccountService().setResponseRingBuffer(responseRingBuffer);
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getOrderService().setSequencerRingBuffer(sequencerRingBuffer);
            dispatcher.getOrderService().setResponseRingBuffer(responseRingBuffer);
        }
    }

    private SequencerDispatcher getSequencerDispatcher() {
        return new SequencerDispatcher();
    }

    private List<MatchDispatcher> getOrderDispatchers() {
        List<MatchDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new MatchDispatcher(10, i));
        }
        return dispatchers;
    }

    public void increase(IncreaseRequest request, StreamObserver<IncreaseResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        sequencerRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.INCREASE);
            Increase increase = message.payload.asIncrease;
            increase.accountId.set(request.getAccountId());
            increase.currencyId.set((short) request.getCurrencyId());
            increase.amount.set(request.getAmount());
        });
    }

    public void decrease(DecreaseRequest request, StreamObserver<DecreaseResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        sequencerRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.DECREASE);
            Decrease decrease = message.payload.asDecrease;
            decrease.accountId.set(request.getAccountId());
            decrease.currencyId.set((short) request.getCurrencyId());
            decrease.amount.set(request.getAmount());
        });
    }

    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        sequencerRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.PLACE_ORDER);
            PlaceOrder placeOrder = message.payload.asPlaceOrder;
            placeOrder.symbolId.set((short) request.getSymbolId());
            placeOrder.accountId.set(request.getAccountId());
            placeOrder.type.set(request.getType() == PlaceOrderRequest.Type.LIMIT ?
                    OrderType.LIMIT : OrderType.MARKET);
            placeOrder.side.set(request.getSide() == PlaceOrderRequest.Side.BID ?
                    OrderSide.BID : OrderSide.ASK);
            placeOrder.price.set(request.getPrice());
            placeOrder.quantity.set(request.getQuantity());
            placeOrder.volume.set(request.getVolume());
        });
    }

    private class ResponseEventHandler implements EventHandler<Message> {

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(Message message, long sequence, boolean endOfBatch) {
            long id = message.id.get();
            StreamObserver<Object> observer = (StreamObserver<Object>) observers.get(id);
            EventType type = message.type.get();
            Object response = switch (type) {
                case ORDER_CREATED -> message.payload.asOrderCreated.get();
                case PLACE_ORDER_REJECTED -> message.payload.asPlaceOrderRejected.get();
                case INCREASED -> message.payload.asIncreased.get();
                case DECREASED -> message.payload.asDecreased.get();
                case DECREASE_REJECTED -> message.payload.asDecreaseRejected.get();
                default -> throw new RuntimeException();
            };
            observer.onNext(response);
            observer.onCompleted();
        }
    }
}
