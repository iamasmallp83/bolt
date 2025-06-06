package com.cmex.bolt.spot.service;


import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.*;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.repository.impl.OrderBookRepository;
import com.cmex.bolt.spot.repository.impl.SymbolRepository;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.cmex.bolt.spot.util.Result;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

public class MatchService {

    @Setter
    private RingBuffer<Message> sequencerRingBuffer;

    @Setter
    private RingBuffer<Message> responseRingBuffer;

    private final OrderIdGenerator generator;

    private final SymbolRepository symbolRepository;
    private final OrderBookRepository orderBookRepository;

    public MatchService() {
        generator = new OrderIdGenerator();
        symbolRepository = new SymbolRepository();
        orderBookRepository = new OrderBookRepository();
    }

    public Optional<Symbol> getSymbol(int symbolId) {
        return symbolRepository.get(symbolId);
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //start to match
        Optional<OrderBook> optional = orderBookRepository.get(placeOrder.symbolId.get());
        optional.ifPresentOrElse(orderBook -> {
            // 使用OrderBook的对象池创建Order
            Order order = orderBook.acquireAndInitOrder(
                    generator.nextId(placeOrder.symbolId.get()),
                    placeOrder.accountId.get(),
                    placeOrder.type.get() == OrderType.LIMIT ? Order.OrderType.LIMIT : Order.OrderType.MARKET,
                    placeOrder.side.get() == OrderSide.BID ? Order.OrderSide.BID : Order.OrderSide.ASK,
                    placeOrder.price.get(),
                    placeOrder.quantity.get(),
                    placeOrder.volume.get(),
                    placeOrder.frozen.get(),
                    placeOrder.takerRate.get(),
                    placeOrder.makerRate.get()
            );
            
            Result<List<Ticket>> result = orderBook.match(order);
            if (result.isSuccess()) {
                long totalQuantity = 0;
                long totalVolume = 0;
                List<Ticket> tickets = result.value();
                
                for (Ticket ticket : tickets) {
                    sequencerRingBuffer.publishEvent((message, sequence) -> {
                        setClearedMessage(ticket.getMaker(), false, ticket.getQuantity(), ticket.getVolume(), message);
                    });
                    totalQuantity += ticket.getQuantity();
                    totalVolume += ticket.getVolume();
                }
                
                // 批量释放Ticket对象回池
                orderBook.releaseTickets(tickets);
                
                long finalTotalQuantity = totalQuantity;
                long finalTotalVolume = totalVolume;
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                    setClearedMessage(order, true, finalTotalQuantity, finalTotalVolume, message);
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
        Optional<OrderBook> optional = orderBookRepository.get(symbolId);
        optional.ifPresentOrElse(orderBook -> {
            Result<Order> result = orderBook.cancel(orderId);
            if (result.isSuccess()) {
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                    message.id.set(messageId);
                    message.type.set(EventType.UNFREEZE);
                    Order order = result.value();
                    message.payload.asUnfreeze.accountId.set(order.getAccountId());
                    message.payload.asUnfreeze.currencyId.set(order.getPayCurrency().getId());
                    message.payload.asUnfreeze.amount.set(order.getUnfreezeAmount());
                });
                responseRingBuffer.publishEvent((message, sequence) -> {
                    message.id.set(messageId);
                    message.type.set(EventType.ORDER_CANCELED);
                });
            } else {
                responseRingBuffer.publishEvent((message, sequence) -> {
                    message.id.set(messageId);
                    message.type.set(EventType.CANCEL_ORDER_REJECTED);
                    message.payload.asPlaceOrderRejected.reason.set(result.reason());
                });
            }

        }, () -> responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.CANCEL_ORDER_REJECTED);
            message.payload.asPlaceOrderRejected.reason.set(RejectionReason.SYMBOL_NOT_EXIST);
        }));
    }

    public DepthDto getDepth(int symbolId) {
        return orderBookRepository.get(symbolId).map(OrderBook::getDepth).orElse(DepthDto.builder().build());
    }

    private void setClearedMessage(Order order, boolean isTaker, long quantity, long volume, Message message) {
        Symbol symbol = order.getSymbol();
        message.type.set(EventType.CLEARED);
        Order.OrderSide side = order.getSide();
        Cleared cleared = message.payload.asCleared;
        cleared.accountId.set(order.getAccountId());
        //支付
        cleared.payCurrencyId.set(symbol.getPayCurrency(side).getId());
        //得到
        cleared.incomeCurrencyId.set(symbol.getIncomeCurrency(side).getId());
        if (side == Order.OrderSide.BID) {
            cleared.payAmount.set(
                    Math.round(volume * (1 + (symbol.isQuoteSettlement() ? order.getRate(isTaker) / Rate.BASE_RATE : 0))));
            cleared.incomeAmount.set(
                    Math.round(quantity * (1 - (symbol.isQuoteSettlement() ? 0 : order.getRate(isTaker) / Rate.BASE_RATE))));
            if (order.isDone() && order.getSide() == Order.OrderSide.BID && order.left() > 0) {
                cleared.refundAmount.set(order.left());
            }
        } else {
            cleared.payAmount.set(quantity);
            cleared.incomeAmount.set(Math.round(volume * (1 - (order.getRate(isTaker) / Rate.BASE_RATE))));
        }
    }

}
