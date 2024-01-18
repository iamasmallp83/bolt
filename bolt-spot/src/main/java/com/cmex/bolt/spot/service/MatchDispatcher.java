package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.EventHandler;

public class MatchDispatcher implements EventHandler<Message> {

    private int amount;
    private int partition;
    private final MatchService matchService;

    public MatchDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.matchService = new MatchService();
    }

    public int getPartition() {
        return partition;
    }

    public MatchService getOrderService() {
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
}