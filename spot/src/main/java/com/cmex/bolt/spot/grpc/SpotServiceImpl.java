package com.cmex.bolt.spot.grpc;


import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.cmex.bolt.spot.domain.AccountDispatcher;
import com.cmex.bolt.spot.domain.OrderDispatcher;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SpotServiceImpl extends SpotServiceGrpc.SpotServiceImplBase {
    private final RingBuffer<Message> accountRingBuffer;
    private final RingBuffer<Message> orderRingBuffer;
    private final RingBuffer<Message> responseRingBuffer;

    private final AtomicLong observerId = new AtomicLong();
    private final ConcurrentHashMap<Long, StreamObserver> observers = new ConcurrentHashMap<>();

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
        responseDisruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            long id = event.observerId.get();
            observers.get(id).onNext(event.payload);
            observers.get(id).onCompleted();
        });
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

    @Override
    public void deposit(SpotServiceProto.DepositRequest request, StreamObserver<GenericResponseProto.GenericResponse> responseObserver) {
        long id = observerId.incrementAndGet();
        observers.put(id, responseObserver);
        accountRingBuffer.publishEvent((event, sequence) -> {
            event.type.set(EventType.DEPOSIT);
            event.observerId.set(id);
            event.payload.asWithdraw.accountId.set(request.getAccountId());
            event.payload.asWithdraw.currencyId.set((short) request.getCurrencyId());
            event.payload.asWithdraw.amount.set(request.getAmount());
        });
//        GenericResponseProto.GenericResponse response = GenericResponseProto.GenericResponse.newBuilder()
//                .setCode(1)
//                .setMessage("success")
//                .build();
//        responseObserver.onNext(response);
//        responseObserver.onCompleted();
    }

    @Override
    public void withdraw(SpotServiceProto.WithdrawRequest request, StreamObserver<GenericResponseProto.GenericResponse> responseObserver) {
        GenericResponseProto.GenericResponse response = GenericResponseProto.GenericResponse.newBuilder()
                .setCode(1)
                .setMessage("success")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
