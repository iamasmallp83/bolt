package com.cmex.bolt.spot.grpc;


import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import com.cmex.bolt.spot.domain.AccountDispatcher;
import com.cmex.bolt.spot.domain.OrderDispatcher;
import io.grpc.stub.StreamObserver;

public class SpotServiceImpl extends SpotServiceGrpc.SpotServiceImplBase {
    private final RingBuffer<Message> accountRingBuffer;
    private final RingBuffer<Message> orderRingBuffer;

    public SpotServiceImpl() {
        int bufferSize = 1024;
        Disruptor<Message> accountDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        Disruptor<Message> orderDisruptor =
                new Disruptor<>(Message.FACTORY, bufferSize, DaemonThreadFactory.INSTANCE);
        AccountDispatcher[] accountDispatchers = new AccountDispatcher[4];
        OrderDispatcher[] orderDispatchers = new OrderDispatcher[4];
        for (int i = 0; i < 4; i++) {
            accountDispatchers[i] = new AccountDispatcher(i);
            orderDispatchers[i] = new OrderDispatcher(i);
        }
        accountDisruptor.handleEventsWith(accountDispatchers);
        orderDisruptor.handleEventsWith(orderDispatchers);
        accountRingBuffer = accountDisruptor.start();
        orderRingBuffer = orderDisruptor.start();
        for (int i = 0; i < 4; i++) {
            accountDispatchers[i].getAccountService().setOrderRingBuffer(orderRingBuffer);
            orderDispatchers[i].getOrderService().setAccountRingBuffer(accountRingBuffer);
        }
    }

    @Override
    public void deposit(SpotServiceProto.DepositRequest request, StreamObserver<GenericResponseProto.GenericResponse> responseObserver) {
        accountRingBuffer.publishEvent((event, sequence) -> {
            event.type.set(EventType.DEPOSIT);
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
