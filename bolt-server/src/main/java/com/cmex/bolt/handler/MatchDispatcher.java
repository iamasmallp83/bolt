package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.service.MatchService;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;

public class MatchDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {

    private final int amount;
    @Getter
    private final int partition;
    @Getter
    private final MatchService matchService;

    public MatchDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.matchService = new MatchService();
    }

    public void onEvent(NexusWrapper message, long sequence, boolean endOfBatch) {
//        EventType type = message.type.get();
//
//        switch (type) {
//            case CANCEL_ORDER:
//                if (partition == OrderIdGenerator.getSymbolId(message.payload.asCancelOrder.orderId.get()) % amount) {
//                    matchService.on(message.id.get(), message.payload.asCancelOrder);
//                }
//                break;
//            case PLACE_ORDER:
//                if (partition == message.payload.asPlaceOrder.symbolId.get() % amount) {
//                    matchService.on(message.id.get(), message.payload.asPlaceOrder);
//                }
//                break;
//            default:
//                break;
//        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(MatchDispatcher.class.getSimpleName() + "-" + partition + "-thread");
    }

    @Override
    public void onShutdown() {

    }
}