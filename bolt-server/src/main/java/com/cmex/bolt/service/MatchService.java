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
import org.capnproto.MessageBuilder;

import java.util.List;
import java.util.Optional;

public class MatchService {

    @Setter
    private RingBuffer<NexusWrapper> sequencerRingBuffer;

    @Setter
    private RingBuffer<NexusWrapper> responseRingBuffer;

    private final OrderIdGenerator generator;

    private final SymbolRepository symbolRepository;

    private final Transfer transfer;

    private final int group;

    public MatchService(int group) {
        generator = new OrderIdGenerator();
        symbolRepository = SymbolRepository.getInstance();
        transfer = new Transfer();
        this.group = group;
    }

    public Optional<Symbol> getSymbol(int symbolId) {
        return symbolRepository.get(symbolId);
    }

    public void on(long messageId, Nexus.PlaceOrder.Reader placeOrder) {
        Optional<Symbol> symbolOptional = symbolRepository.get(placeOrder.getSymbolId());
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Order order = getOrder(symbol, placeOrder);
            Result<List<Ticket>> result = orderBook.match(order);
            if (result.isSuccess()) {
                long totalQuantity = 0;
                long totalVolume = 0;
                for (Ticket ticket : result.value()) {
                    sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                        MessageBuilder builder = createClearMessage(ticket.getMaker(), false,
                                ticket.getQuantity(), ticket.getVolume());
                        wrapper.setPartition(ticket.getMaker().getAccountId());
                        transfer.serialize(builder, wrapper.getBuffer());
                    });
                    totalQuantity += ticket.getQuantity();
                    totalVolume += ticket.getVolume();
                }
                long finalTotalQuantity = totalQuantity;
                long finalTotalVolume = totalVolume;
                sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                    MessageBuilder builder = createClearMessage(order, true, finalTotalQuantity,
                            finalTotalVolume);
                    wrapper.setPartition(order.getAccountId());
                    transfer.serialize(builder, wrapper.getBuffer());
                });
            }
            responseRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(messageId);
                transfer.writeOrder(order, wrapper.getBuffer());
            });
        }, () -> responseRingBuffer.publishEvent((wrapper, sequence) -> {
            wrapper.setId(messageId);
            transfer.writeFailed(Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.SYMBOL_NOT_EXIST,
                    wrapper.getBuffer());
        }));
    }

    public void on(long messageId, Nexus.CancelOrder.Reader cancelOrder) {
        long orderId = cancelOrder.getOrderId();
        int symbolId = OrderIdGenerator.getSymbolId(orderId);
        Optional<Symbol> symbolOptional = symbolRepository.get(symbolId);
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Result<Order> result = orderBook.cancel(orderId);
            if (result.isSuccess()) {
                sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                    wrapper.setId(messageId);
                    Order order = result.value();
                    wrapper.setPartition(order.getAccountId() % group);
                    transfer.writeUnfreeze(order, wrapper.getBuffer());
                });
                responseRingBuffer.publishEvent((wrapper, sequence) -> {
                    wrapper.setId(messageId);
                    transfer.writeCancelOrder(cancelOrder, wrapper.getBuffer());
                });
            } else {
                responseRingBuffer.publishEvent((wrapper, sequence) -> {
                    wrapper.setId(messageId);
                    transfer.writeFailed(Nexus.EventType.CANCEL_ORDER_REJECTED, Nexus.RejectionReason.ORDER_NOT_EXIST,
                            wrapper.getBuffer());
                });
            }
        }, () -> responseRingBuffer.publishEvent((wrapper, sequence) -> {
            wrapper.setId(messageId);
            transfer.writeFailed(Nexus.EventType.CANCEL_ORDER_REJECTED, Nexus.RejectionReason.ORDER_NOT_EXIST,
                    wrapper.getBuffer());
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

    private MessageBuilder createClearMessage(Order order, boolean isTaker, long quantity, long volume) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Clear.Builder clear = builder.getPayload().initClear();
        Symbol symbol = order.getSymbol();
        Order.OrderSide side = order.getSide();
        clear.setAccountId(order.getAccountId());
        //支付
        clear.setPayCurrencyId(symbol.getPayCurrency(side).getId());
        //得到
        clear.setIncomeCurrencyId(symbol.getIncomeCurrency(side).getId());
        if (side == Order.OrderSide.BID) {
            clear.setPayAmount(
                    Math.round(volume * (1 + (symbol.isQuoteSettlement() ? order.getRate(isTaker) / Rate.BASE_RATE : 0))));
            clear.setIncomeAmount(
                    Math.round(quantity * (1 - (symbol.isQuoteSettlement() ? 0 : order.getRate(isTaker) / Rate.BASE_RATE))));
            if (order.isDone() && order.getSide() == Order.OrderSide.BID && order.left() > 0) {
                clear.setRefundAmount(order.left());
            }
        } else {
            clear.setPayAmount(quantity);
            clear.setIncomeAmount(Math.round(volume * (1 - (order.getRate(isTaker) / Rate.BASE_RATE))));
        }
        return messageBuilder;
    }

    private Order getOrder(Symbol symbol, Nexus.PlaceOrder.Reader placeOrder) {
        return Order.builder()
                .symbol(symbol)
                .id(generator.nextId(placeOrder.getSymbolId()))
                .accountId(placeOrder.getAccountId())
                .type(placeOrder.getType() == Nexus.OrderType.LIMIT ? Order.OrderType.LIMIT : Order.OrderType.MARKET)
                .side(placeOrder.getSide() == Nexus.OrderSide.BID ? Order.OrderSide.BID : Order.OrderSide.ASK)
                .price(placeOrder.getPrice())
                .quantity(placeOrder.getQuantity())
                .volume(placeOrder.getVolume())
                .takerRate(placeOrder.getTakerRate())
                .makerRate(placeOrder.getMakerRate())
                .build();
    }

}
