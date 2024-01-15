package com.cmex.bolt.spot.grpc;


import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.domain.AccountDispatcher;
import com.cmex.bolt.spot.domain.OrderDispatcher;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SpotServiceImpl extends SpotServiceGrpc.SpotServiceImplBase {

    private final GenericResponseProto.GenericResponse success = GenericResponseProto.GenericResponse.newBuilder()
            .setCode(1).setMessage("success").build();
    private final GenericResponseProto.GenericResponse systemError = GenericResponseProto.GenericResponse.newBuilder()
            .setCode(0).setMessage("system error").build();
    private final RingBuffer<Message> accountRingBuffer;
    private final RingBuffer<Message> orderRingBuffer;
    private final RingBuffer<Message> responseRingBuffer;

    private final AtomicLong requestId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver<GenericResponseProto.GenericResponse>> observers = new ConcurrentHashMap<>();

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
        orderRingBuffer = orderDisruptor.start();
        responseRingBuffer = responseDisruptor.start();
        for (int i = 0; i < 4; i++) {
            accountDispatchers[i].getAccountService().setOrderRingBuffer(orderRingBuffer);
            orderDispatchers[i].getOrderService().setAccountRingBuffer(accountRingBuffer);
            accountDispatchers[i].getAccountService().setResponseRingBuffer(responseRingBuffer);
            orderDispatchers[i].getOrderService().setResponseRingBuffer(responseRingBuffer);
        }
    }

    public void deposit(SpotServiceProto.DepositRequest request, StreamObserver<GenericResponseProto.GenericResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.DEPOSIT);
            message.payload.asDeposit.accountId.set(request.getAccountId());
            message.payload.asDeposit.currencyId.set((short) request.getCurrencyId());
            message.payload.asDeposit.amount.set(request.getAmount());
        });
    }

    public void withdraw(SpotServiceProto.WithdrawRequest request, StreamObserver<GenericResponseProto.GenericResponse> responseObserver) {
        long id = requestId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(id);
            message.type.set(EventType.WITHDRAW);
            message.payload.asWithdraw.accountId.set(request.getAccountId());
            message.payload.asWithdraw.currencyId.set((short) request.getCurrencyId());
            message.payload.asWithdraw.amount.set(request.getAmount());
        });
    }

    private class ResponseEventHandler implements EventHandler<Message> {

        @Override
        public void onEvent(Message message, long sequence, boolean endOfBatch) throws Exception {
            long id = message.id.get();
            StreamObserver<GenericResponseProto.GenericResponse> observer = observers.get(id);
            EventType type = message.type.get();
            GenericResponseProto.GenericResponse response;
            switch (type) {
                case ORDER_CREATE:
                    response = message.payload.asOrderCreated.get();
                    break;
                case PLACE_ORDER_REJECTED:
                    response = message.payload.asPlaceOrderRejected.get();
                    break;
                case DEPOSITED:
                    response = message.payload.asDeposited.get();
                    break;
                case WITHDRAWN:
                    response = message.payload.asWithdrawn.get();
                    break;
                case WITHDRAW_REJECTED:
                    response = message.payload.asWithdrawRejected.get();
                    break;
                default:
                    response = systemError;
            }
            observer.onNext(response);
            observer.onCompleted();
        }
    }
}
