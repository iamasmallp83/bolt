package com.cmex.bolt.spot.service;


import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.Order;
import com.cmex.bolt.spot.domain.OrderBook;
import com.cmex.bolt.spot.domain.Symbol;
import com.cmex.bolt.spot.domain.Ticket;
import com.cmex.bolt.spot.repository.impl.OrderBookRepository;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.RingBuffer;

import java.util.List;
import java.util.Optional;

public class MatchService {

    private RingBuffer<Message> sequencerRingBuffer;

    private RingBuffer<Message> responseRingBuffer;

    private final OrderIdGenerator generator;

    private final OrderBookRepository repository;

    public MatchService() {
        generator = new OrderIdGenerator();
        repository = new OrderBookRepository();
        Symbol btcusdt = Symbol.getSymbol((short) 1);
        Symbol ethusdt = Symbol.getSymbol((short) 2);
        repository.getOrCreate(btcusdt.getId(), new OrderBook(btcusdt));
        repository.getOrCreate(ethusdt.getId(), new OrderBook(ethusdt));
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //start to match
        Optional<OrderBook> optional = repository.get(placeOrder.symbolId.get());
        optional.ifPresentOrElse(orderBook -> {
            Order order = getOrder(orderBook.getSymbol(), placeOrder);
            List<Ticket> tickets = orderBook.match(order);
            if (!tickets.isEmpty()) {
                long totalQuantity = 0;
                long totalVolume = 0;
                for (Ticket ticket : tickets) {
                    sequencerRingBuffer.publishEvent((message, sequence) -> {
                        setMessage(ticket.getMaker(), ticket.getQuantity(), ticket.getVolume(), message);
                    });
                    totalQuantity += ticket.getQuantity();
                    totalVolume += ticket.getVolume();
                }
                long finalTotalQuantity = totalQuantity;
                long finalTotalVolume = totalVolume;
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                    setMessage(order, finalTotalQuantity, finalTotalVolume, message);
                });
            }
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.ORDER_CREATED);
                message.payload.asOrderCreated.orderId.set(order.getId());
            });
        }, () -> responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.PLACE_ORDER_REJECTED);
            message.payload.asPlaceOrderRejected.reason.set(RejectionReason.SYMBOL_NOT_EXIST);
        }));
    }

    public void on(long messageId, CancelOrder cancelOrder) {
        long orderId = cancelOrder.orderId.get();
        int symbolId = OrderIdGenerator.getSymbolId(orderId);
        Optional<OrderBook> optional = repository.get(symbolId);
        optional.ifPresentOrElse(orderBook -> {
            Order order = orderBook.cancel(orderId);
            if (order != null) {
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                    message.id.set(messageId);
                    message.type.set(EventType.UNFREEZE);
                    message.payload.asUnfreeze.accountId.set(order.getAccountId());
                    message.payload.asUnfreeze.currencyId.set(order.getPayCurrency().getId());
                    message.payload.asUnfreeze.amount.set(order.getUnfreezeAmount());
                });
            } else {
                responseRingBuffer.publishEvent((message, sequence) -> {
                    message.id.set(messageId);
                    message.type.set(EventType.PLACE_ORDER_REJECTED);
                    message.payload.asPlaceOrderRejected.reason.set(RejectionReason.ORDER_NOT_EXIST);
                });
            }

        }, () -> responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.PLACE_ORDER_REJECTED);
            message.payload.asPlaceOrderRejected.reason.set(RejectionReason.SYMBOL_NOT_EXIST);
        }));
    }

    public void setSequencerRingBuffer(RingBuffer<Message> sequencerRingBuffer) {
        this.sequencerRingBuffer = sequencerRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<Message> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }

    private Order getOrder(Symbol symbol, PlaceOrder placeOrder) {
        return Order.builder()
                .symbol(symbol)
                .id(generator.nextId(placeOrder.symbolId.get()))
                .accountId(placeOrder.accountId.get())
                .type(placeOrder.type.get() == OrderType.LIMIT ? Order.OrderType.LIMIT : Order.OrderType.MARKET)
                .side(placeOrder.side.get() == OrderSide.BID ? Order.OrderSide.BID : Order.OrderSide.ASK)
                .price(placeOrder.price.get())
                .quantity(placeOrder.quantity.get())
                .volume(placeOrder.volume.get())
                .build();
    }

    private void setMessage(Order order, long quantity, long volume, Message message) {
        Symbol symbol = order.getSymbol();
        message.type.set(EventType.CLEARED);
        Order.OrderSide side = order.getSide();
        message.payload.asCleared.accountId.set(order.getAccountId());
        //支付
        message.payload.asCleared.payCurrencyId.set(symbol.getPayCurrency(side).getId());
        //得到
        message.payload.asCleared.incomeCurrencyId.set(symbol.getIncomeCurrency(side).getId());
        if (side == Order.OrderSide.BID) {
            message.payload.asCleared.payAmount.set(volume);
            message.payload.asCleared.incomeAmount.set(quantity);
        } else {
            message.payload.asCleared.payAmount.set(quantity);
            message.payload.asCleared.incomeAmount.set(volume);
        }
    }

}
