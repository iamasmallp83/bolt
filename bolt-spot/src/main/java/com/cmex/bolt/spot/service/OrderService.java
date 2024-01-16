package com.cmex.bolt.spot.service;


import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.Order;
import com.cmex.bolt.spot.domain.OrderBook;
import com.cmex.bolt.spot.repository.impl.OrderBookRepository;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.RingBuffer;

import java.math.BigDecimal;
import java.util.Optional;

import static javax.swing.UIManager.get;

public class OrderService {

    private RingBuffer<Message> accountRingBuffer;

    private RingBuffer<Message> responseRingBuffer;

    private OrderIdGenerator generator;

    private OrderBookRepository repository;

    public OrderService() {
        generator = new OrderIdGenerator();
        this.repository = new OrderBookRepository();
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //start to match
        Optional<OrderBook> optional = repository.get(placeOrder.symbolId.get());
        optional.ifPresentOrElse(orderBook -> {
            Order order = getOrder(placeOrder);
            orderBook.match(order);
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.ORDER_CREATED);
                message.payload.asOrderCreated.orderId.set(order.getId());
            });
        }, () -> {
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.PLACE_ORDER_REJECTED);
                message.payload.asPlaceOrderRejected.reason.set(RejectionReason.SYMBOL_NOT_EXIST);
            });
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

    private Order getOrder(PlaceOrder placeOrder) {
        return Order.builder()
                .id(generator.nextId(placeOrder.symbolId.get()))
                .accountId(placeOrder.accountId.get())
                .side(placeOrder.side.get() == OrderSide.BID ? Order.OrderSide.BID : Order.OrderSide.ASK)
                .price(new BigDecimal(placeOrder.price.get()))
                .size(new BigDecimal(placeOrder.size.get()))
                .build();
    }
}
