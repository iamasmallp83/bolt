package com.cmex.bolt.spot.grpc;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.AccountDispatcher;
import com.cmex.bolt.spot.domain.OrderDispatcher;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.cmex.bolt.spot.grpc.SpotServiceGrpc.*;
import static com.cmex.bolt.spot.grpc.SpotServiceProto.*;

public class SpotServiceImpl extends SpotServiceImplBase {

    private final RingBuffer<Message> accountRingBuffer;

    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<?>> observers = new ConcurrentHashMap<>();

    public SpotServiceImpl() {
        int bufferSize = 1024;
        Disruptor<Message> accountDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> orderDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> responseDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        AccountDispatcher[] accountDispatchers = new AccountDispatcher[4];
        OrderDispatcher[] orderDispatchers = new OrderDispatcher[4];
        for (int i = 0; i < 4; i++) {
            accountDispatchers[i] = new AccountDispatcher(i);
            orderDispatchers[i] = new OrderDispatcher(i);
        }
        accountDisruptor.handleEventsWith(accountDispatchers);
        orderDisruptor.handleEventsWith(orderDispatchers);
        responseDisruptor.handleEventsWith(new ResponseEventHandler());
        accountRingBuffer = accountDisruptor.start();
        RingBuffer<Message> orderRingBuffer = orderDisruptor.start();
        RingBuffer<Message> responseRingBuffer = responseDisruptor.start();
        for (int i = 0; i < 4; i++) {
            accountDispatchers[i].getAccountService().setOrderRingBuffer(orderRingBuffer);
            orderDispatchers[i].getOrderService().setAccountRingBuffer(accountRingBuffer);
            accountDispatchers[i].getAccountService().setResponseRingBuffer(responseRingBuffer);
            orderDispatchers[i].getOrderService().setResponseRingBuffer(responseRingBuffer);
        }
    }

    public void deposit(DepositRequest request, StreamObserver<DepositResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.DEPOSIT);
            Deposit deposit = message.payload.asDeposit;
            deposit.accountId.set(request.getAccountId());
            deposit.currencyId.set((short) request.getCurrencyId());
            deposit.amount.set(request.getAmount());
        });
    }

    public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.WITHDRAW);
            Withdraw withdraw = message.payload.asWithdraw;
            withdraw.accountId.set(request.getAccountId());
            withdraw.currencyId.set((short) request.getCurrencyId());
            withdraw.amount.set(request.getAmount());
        });
    }

    public void placeOrder(PlaceOrderRequest request, StreamObserver<PlaceOrderResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
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
            placeOrder.size.set(request.getSize());
            placeOrder.funds.set(request.getFunds());
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
                case DEPOSITED -> message.payload.asDeposited.get();
                case WITHDRAWN -> message.payload.asWithdrawn.get();
                case WITHDRAW_REJECTED -> message.payload.asWithdrawRejected.get();
                default -> throw new RuntimeException();
            };
            observer.onNext(response);
            observer.onCompleted();
        }
    }
}
