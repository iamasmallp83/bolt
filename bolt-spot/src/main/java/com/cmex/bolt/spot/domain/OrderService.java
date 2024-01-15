package com.cmex.bolt.spot.domain;


import com.cmex.bolt.spot.api.CancelOrder;
import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.api.PlaceOrder;
import com.lmax.disruptor.RingBuffer;

public class OrderService {

    private RingBuffer<Message> accountRingBuffer;

    private RingBuffer<Message> responseRingBuffer;

    public OrderService() {
    }

    public void on(PlaceOrder placeOrder) {
        //start to match
        accountRingBuffer.publishEvent((event, sequence) -> {
            event.type.set(EventType.UNFREEZE);
            event.payload.asUnfreeze.accountId.set(placeOrder.accountId.get());
            if (placeOrder.buy.get()) {
                event.payload.asUnfreeze.amount.set(placeOrder.price.get() * placeOrder.size.get());
            } else {
                event.payload.asUnfreeze.amount.set(placeOrder.size.get());
            }
        });
    }

    public void on(CancelOrder cancelOrder) {
    }

    public void setAccountRingBuffer(RingBuffer<Message> accountRingBuffer) {
        this.accountRingBuffer = accountRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<Message> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }
}
