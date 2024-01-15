package com.cmex.bolt.spot.domain;

import com.lmax.disruptor.EventHandler;
import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;

public class OrderDispatcher implements EventHandler<Message> {

    private int partition;
    private final OrderService orderService;

    public OrderDispatcher(int partition) {
        this.partition = partition;
        this.orderService = new OrderService();
    }

    public int getPartition() {
        return partition;
    }

    public OrderService getOrderService() {
        return orderService;
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();

        switch (type) {
            case CANCEL_ORDER:
                if(partition == message.payload.asCancelOrder.orderId.get() % 4) {
                    orderService.on(message.payload.asCancelOrder);
                }
                break;
            case PLACE_ORDER:
                if(partition == message.payload.asPlaceOrder.symbolId.get() % 4) {
                    orderService.on(message.payload.asPlaceOrder);
                }
                break;
        }
    }
}