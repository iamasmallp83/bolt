package com.cmex.bolt.spot.domain;


import com.cmex.bolt.spot.api.CancelOrder;
import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.api.PlaceOrder;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;

public class OrderService {

    private RingBuffer<Message> accountRingBuffer;

    private RingBuffer<Message> responseRingBuffer;

    private OrderIdGenerator generator;

    public OrderService() {
        generator = new OrderIdGenerator();
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //start to match
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.ORDER_CREATED);
            message.payload.asOrderCreated.orderId.set(generator.nextId(placeOrder.symbolId.get()));
        });
    }

    public void on(long messageId, CancelOrder cancelOrder) {
    }

    public void setAccountRingBuffer(RingBuffer<Message> accountRingBuffer) {
        this.accountRingBuffer = accountRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<Message> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }
}
