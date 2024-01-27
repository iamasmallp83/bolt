package com.cmex.bolt.spot.handler;

import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.service.MatchService;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class MatchDispatcher implements EventHandler<Message>, LifecycleAware {

    private final int amount;
    private final int partition;
    private final MatchService matchService;

    public MatchDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.matchService = new MatchService();
    }

    public int getPartition() {
        return partition;
    }

    public MatchService getMatchService() {
        return matchService;
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();

        switch (type) {
            case CANCEL_ORDER:
                if (partition == OrderIdGenerator.getSymbolId(message.payload.asCancelOrder.orderId.get()) % amount) {
                    matchService.on(message.id.get(), message.payload.asCancelOrder);
                }
                break;
            case PLACE_ORDER:
                if (partition == message.payload.asPlaceOrder.symbolId.get() % amount) {
                    matchService.on(message.id.get(), message.payload.asPlaceOrder);
                }
                break;
        }
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