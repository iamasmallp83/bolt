package com.cmex.bolt.spot.grpc;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.service.AccountService;
import com.cmex.bolt.spot.service.MatchDispatcher;
import com.cmex.bolt.spot.service.MatchService;
import com.cmex.bolt.spot.service.SequencerDispatcher;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.cmex.bolt.spot.grpc.SpotServiceGrpc.SpotServiceImplBase;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.*;

public class SpotServiceImpl extends SpotServiceImplBase {

    private final RingBuffer<Message> sequencerRingBuffer;
    private final SequencerDispatcher sequencerDispatcher;

    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers;

    public SpotServiceImpl() {
        observers = new ConcurrentHashMap<>();
        int bufferSize = 1024;
        Disruptor<Message> sequencerDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> matchDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> responseDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        sequencerDispatcher = createSequencerDispatcher();
        List<MatchDispatcher> matchDispatchers = createOrderDispatchers();
        sequencerDisruptor.handleEventsWith(sequencerDispatcher);
        matchDisruptor.handleEventsWith(matchDispatchers.toArray(new MatchDispatcher[0]));
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        sequencerRingBuffer = sequencerDisruptor.start();
        RingBuffer<Message> matchRingBuffer = matchDisruptor.start();
        RingBuffer<Message> responseRingBuffer = responseDisruptor.start();
        sequencerDispatcher.getAccountService().setMatchRingBuffer(matchRingBuffer);
        sequencerDispatcher.getAccountService().setResponseRingBuffer(responseRingBuffer);
        MatchService[] matchServices = new MatchService[10];
        int i = 0;
        for (MatchDispatcher dispatcher : matchDispatchers) {
            dispatcher.getMatchService().setSequencerRingBuffer(sequencerRingBuffer);
            dispatcher.getMatchService().setResponseRingBuffer(responseRingBuffer);
            matchServices[i++] = dispatcher.getMatchService();
        }
        sequencerDispatcher.setMatchServices(matchServices);
    }

    private SequencerDispatcher createSequencerDispatcher() {
        return new SequencerDispatcher();
    }

    private List<MatchDispatcher> createOrderDispatchers() {
        List<MatchDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new MatchDispatcher(10, i));
        }
        return dispatchers;
    }

    @Override
    public void getAccount(GetAccountRequest request, StreamObserver<GetAccountResponse> responseObserver) {
        AccountService service = sequencerDispatcher.getAccountService();
        Map<Integer, SpotServiceProto.Balance> balances =
                service.getBalances(request.getAccountId(), request.getCurrencyId()).entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> SpotServiceProto.Balance.newBuilder()
                                        .setFrozen(entry.getValue().getFrozen())
                                        .setAvailable(entry.getValue().available())
                                        .setValue(entry.getValue().getValue())
                                        .build()
                        ));
        GetAccountResponse getAccountResponse = GetAccountResponse.newBuilder()
                .setCode(1)
                .putAllData(balances)
                .build();
        responseObserver.onNext(getAccountResponse);
        responseObserver.onCompleted();
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
            placeOrder.takerRate.set(request.getTakerRate());
            placeOrder.makerRate.set(request.getMakerRate());
        });
    }

    @Override
    public void cancelOrder(CancelOrderRequest request, StreamObserver<CancelOrderResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        sequencerRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.CANCEL_ORDER);
            message.payload.asCancelOrder.orderId.set(request.getOrderId());
        });
    }

    @Override
    public void getDepth(GetDepthRequest request, StreamObserver<GetDepthResponse> responseObserver) {
        MatchService[] matchServices = sequencerDispatcher.getMatchServices();
        int symbolId = request.getSymbolId();
        MatchService matchService = matchServices[symbolId % 10];
        DepthDto dto = matchService.getDepth(symbolId);
        GetDepthResponse response = GetDepthResponse.newBuilder()
                .setCode(1)
                .setData(Depth.newBuilder().putAllAsks(dto.getAsks()).putAllBids(dto.getBids()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private class ResponseEventHandler implements EventHandler<Message>, LifecycleAware {

        @SuppressWarnings("unchecked")
        @Override
        public void onEvent(Message message, long sequence, boolean endOfBatch) {
            long id = message.id.get();
            if (id > 0) {
                StreamObserver<Object> observer = (StreamObserver<Object>) observers.get(id);
                EventType type = message.type.get();
                Object response = switch (type) {
                    case ORDER_CREATED -> message.payload.asOrderCreated.get();
                    case PLACE_ORDER_REJECTED -> message.payload.asPlaceOrderRejected.get();
                    case ORDER_CANCELED -> message.payload.asOrderCanceled.get();
                    case INCREASED -> message.payload.asIncreased.get();
                    case DECREASED -> message.payload.asDecreased.get();
                    case DECREASE_REJECTED -> message.payload.asDecreaseRejected.get();
                    default -> throw new RuntimeException();
                };
                observer.onNext(response);
                observer.onCompleted();
            }
        }

        @Override
        public void onStart() {
            final Thread currentThread = Thread.currentThread();
            currentThread.setName(ResponseEventHandler.class.getSimpleName() + "-thread");
        }

        @Override
        public void onShutdown() {

        }
    }
}
