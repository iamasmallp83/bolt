package com.cmex.bolt.service;


import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.Result;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;

import java.util.List;
import java.util.Optional;

public class MatchService {

    @Setter
    private RingBuffer<NexusWrapper> sequencerRingBuffer;

    @Setter
    private RingBuffer<NexusWrapper> responseRingBuffer;

    private final OrderIdGenerator generator;

    private final SymbolRepository symbolRepository;

    public MatchService() {
        generator = new OrderIdGenerator();
        symbolRepository = new SymbolRepository();
    }

    public Optional<Symbol> getSymbol(int symbolId) {
        return symbolRepository.get(symbolId);
    }

    public void on(long messageId, Nexus.PlaceOrder placeOrder) {
        //start to match
        Optional<Symbol> symbolOptional = symbolRepository.get(0);
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Order order = orderBook.getOrder(placeOrder);
            Result<List<Ticket>> result = orderBook.match(order);
            if (result.isSuccess()) {
                long totalQuantity = 0;
                long totalVolume = 0;
                for (Ticket ticket : result.value()) {
                    sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                        setClearedMessage(ticket.getMaker(), false, ticket.getQuantity(), ticket.getVolume(), null);
                    });
                    totalQuantity += ticket.getQuantity();
                    totalVolume += ticket.getVolume();
                }
                long finalTotalQuantity = totalQuantity;
                long finalTotalVolume = totalVolume;
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                    setClearedMessage(order, true, finalTotalQuantity, finalTotalVolume, message);
                });
            }
            responseRingBuffer.publishEvent((message, sequence) -> {
            });
        }, () -> responseRingBuffer.publishEvent((message, sequence) -> {
        }));
    }

    public void on(long messageId, Nexus.CancelOrder cancelOrder) {
//        long orderId = cancelOrder.orderId.get();
        long orderId = 0;
        int symbolId = OrderIdGenerator.getSymbolId(orderId);
        Optional<Symbol> symbolOptional = symbolRepository.get(symbolId);
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Result<Order> result = orderBook.cancel(orderId);
            if (result.isSuccess()) {
                sequencerRingBuffer.publishEvent((message, sequence) -> {
                });
                responseRingBuffer.publishEvent((message, sequence) -> {
                });
            } else {
                responseRingBuffer.publishEvent((message, sequence) -> {
                });
            }

        }, () -> responseRingBuffer.publishEvent((message, sequence) -> {
        }));
    }

    public DepthDto getDepth(int symbolId) {
        return symbolRepository.get(symbolId)
                .map(Symbol::getDepth)
                .orElse(createEmptyDepth());
    }

    private DepthDto createEmptyDepth() {
        return DepthDto.builder()
                .symbol("")
                .asks(new java.util.TreeMap<>())
                .bids(new java.util.TreeMap<>())
                .build();
    }

    private void setClearedMessage(Order order, boolean isTaker, long quantity, long volume, NexusWrapper wrapper) {
//        Symbol symbol = order.getSymbol();
//        message.type.set(EventType.CLEARED);
//        Order.OrderSide side = order.getSide();
//        Cleared cleared = message.payload.asCleared;
//        cleared.accountId.set(order.getAccountId());
//        //支付
//        cleared.payCurrencyId.set(symbol.getPayCurrency(side).getId());
//        //得到
//        cleared.incomeCurrencyId.set(symbol.getIncomeCurrency(side).getId());
//        if (side == Order.OrderSide.BID) {
//            cleared.payAmount.set(
//                    Math.round(volume * (1 + (symbol.isQuoteSettlement() ? order.getRate(isTaker) / Rate.BASE_RATE : 0))));
//            cleared.incomeAmount.set(
//                    Math.round(quantity * (1 - (symbol.isQuoteSettlement() ? 0 : order.getRate(isTaker) / Rate.BASE_RATE))));
//            if (order.isDone() && order.getSide() == Order.OrderSide.BID && order.left() > 0) {
//                cleared.refundAmount.set(order.left());
//            }
//        } else {
//            cleared.payAmount.set(quantity);
//            cleared.incomeAmount.set(Math.round(volume * (1 - (order.getRate(isTaker) / Rate.BASE_RATE))));
//        }
    }

}
