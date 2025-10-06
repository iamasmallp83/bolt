package com.cmex.bolt.service;


import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.Result;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;
import org.capnproto.MessageBuilder;

import java.util.List;
import java.math.BigDecimal;
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

    public void on(NexusWrapper wrapper, Nexus.PlaceOrder.Reader placeOrder) {
        long messageId = wrapper.getId();
        Optional<Symbol> symbolOptional = symbolRepository.get(placeOrder.getSymbolId());
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Order order = getOrder(symbol, placeOrder);
            Result<List<Ticket>> result = orderBook.match(order);
            if (result.isSuccess()) {
                BigDecimal totalQuantity = BigDecimal.ZERO;
                BigDecimal totalVolume = BigDecimal.ZERO;
                for (Ticket ticket : result.value()) {
                    sequencerRingBuffer.publishEvent((sequencerWrapper, sequence) -> {
                        MessageBuilder builder = createClearMessage(symbol, ticket.getMaker(), false,
                                ticket.getQuantity(), ticket.getVolume());
                        sequencerWrapper.setPartition(ticket.getMaker().getAccountId() % group);
                        setSequencerEventType(wrapper, sequencerWrapper);
                        transfer.serialize(builder, sequencerWrapper.getBuffer());
                    });
                    totalQuantity = totalQuantity.add(ticket.getQuantity());
                    totalVolume = totalVolume.add(ticket.getVolume());
                }
                BigDecimal finalTotalQuantity = totalQuantity;
                BigDecimal finalTotalVolume = totalVolume;
                sequencerRingBuffer.publishEvent((sequencerWrapper, sequence) -> {
                    MessageBuilder builder = createClearMessage(symbol, order, true, finalTotalQuantity,
                            finalTotalVolume);
                    sequencerWrapper.setPartition(order.getAccountId() % group);
                    setSequencerEventType(wrapper, sequencerWrapper);
                    transfer.serialize(builder, sequencerWrapper.getBuffer());
                });
            }
            if (wrapper.isJournalEvent()) {
                return;
            }
            responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
                responseWrapper.setId(messageId);
                setResponseEventType(wrapper, responseWrapper);
                transfer.writeOrder(order, responseWrapper.getBuffer());
            });
        }, () -> {
            if (wrapper.isJournalEvent()) {
                return;
            }
            responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
                responseWrapper.setId(messageId);
                setResponseEventType(wrapper, responseWrapper);
                transfer.writeFailed(Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.SYMBOL_NOT_EXIST,
                        responseWrapper.getBuffer());
            });
        });
    }

    public void on(NexusWrapper wrapper, Nexus.CancelOrder.Reader cancelOrder) {
        long messageId = wrapper.getId();
        long orderId = cancelOrder.getOrderId();
        int symbolId = OrderIdGenerator.getSymbolId(orderId);
        Optional<Symbol> symbolOptional = symbolRepository.get(symbolId);
        symbolOptional.ifPresentOrElse(symbol -> {
            OrderBook orderBook = symbol.getOrderBook();
            Result<Order> result = orderBook.cancel(orderId);
            if (result.isSuccess()) {
                sequencerRingBuffer.publishEvent((sequencerWrapper, sequence) -> {
                    sequencerWrapper.setId(messageId);
                    Order order = result.value();
                    sequencerWrapper.setPartition(order.getAccountId() % group);
                    setSequencerEventType(wrapper, sequencerWrapper);
                    transfer.writeUnfreeze(symbol, order, sequencerWrapper.getBuffer());
                });
                if (wrapper.isJournalEvent()) {
                    return;
                }
                responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
                    responseWrapper.setId(messageId);
                    setResponseEventType(wrapper, responseWrapper);
                    transfer.writeCancelOrder(cancelOrder, responseWrapper.getBuffer());
                });
            } else {
                if (wrapper.isJournalEvent()) {
                    return;
                }
                responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
                    responseWrapper.setId(messageId);
                    setResponseEventType(wrapper, responseWrapper);
                    transfer.writeFailed(Nexus.EventType.CANCEL_ORDER_REJECTED, Nexus.RejectionReason.ORDER_NOT_EXIST,
                            responseWrapper.getBuffer());
                });
            }
        }, () -> {
            if (wrapper.isJournalEvent()) {
                return;
            }
            responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
                responseWrapper.setId(messageId);
                setResponseEventType(wrapper, responseWrapper);
                transfer.writeFailed(Nexus.EventType.CANCEL_ORDER_REJECTED, Nexus.RejectionReason.ORDER_NOT_EXIST,
                        wrapper.getBuffer());
            });
        });
    }

    public DepthDto getDepth(int symbolId) {
        return symbolRepository.get(symbolId)
                .map(Symbol::getDepth).get();
    }

    private MessageBuilder createClearMessage(Symbol symbol, Order order, boolean isTaker, BigDecimal quantity, BigDecimal volume) {
        MessageBuilder messageBuilder = new MessageBuilder();
        Nexus.NexusEvent.Builder builder = messageBuilder.initRoot(Nexus.NexusEvent.factory);
        Nexus.Clear.Builder clear = builder.getPayload().initClear();
        Order.Side side = order.getSide();
        clear.setAccountId(order.getAccountId());
        //支付
        clear.setPayCurrencyId(symbol.getPayCurrency(side).getId());
        //得到
        clear.setIncomeCurrencyId(symbol.getIncomeCurrency(side).getId());
        if (side == Order.Side.BID) {
            clear.setPayAmount(
                    volume.multiply(BigDecimal.valueOf(1 + (symbol.isQuoteSettlement() ? order.getFee().get(isTaker) / Rate.BASE_RATE_DOUBLE : 0))).toString());
            clear.setIncomeAmount(
                    quantity.multiply(BigDecimal.valueOf(1 - (symbol.isQuoteSettlement() ? 0 : order.getFee().get(isTaker) / Rate.BASE_RATE_DOUBLE))).toString());
            if (order.isDone() && BigDecimalUtil.gtZero(order.getUnfreezeAmount())) {
                clear.setRefundAmount(order.getUnfreezeAmount().toString());
            }
        } else {
            clear.setPayAmount(quantity.toString());
            clear.setIncomeAmount(volume.multiply(BigDecimal.valueOf(1 - (order.getFee().get(isTaker) / Rate.BASE_RATE_DOUBLE))).toString());
        }
        return messageBuilder;
    }

    private Order getOrder(Symbol symbol, Nexus.PlaceOrder.Reader placeOrder) {
        Order.Specification specification;
        if (placeOrder.getType() == Nexus.OrderType.LIMIT) {
            specification = Order.Specification.limitByQuantity(
                    new BigDecimal(placeOrder.getPrice().toString()),
                    new BigDecimal(placeOrder.getQuantity().toString()));
        } else {
            if (placeOrder.getSide() == Nexus.OrderSide.BID) {
                specification = Order.Specification.marketByAmount(new BigDecimal(placeOrder.getVolume().toString()));
            } else {
                specification = Order.Specification.marketByQuantity(new BigDecimal(placeOrder.getQuantity().toString()));
            }
        }
        return Order.builder()
                .id(generator.nextId(placeOrder.getSymbolId()))
                .symbolId(symbol.getId())
                .accountId(placeOrder.getAccountId())
                .type(placeOrder.getType() == Nexus.OrderType.LIMIT ? Order.Type.LIMIT : Order.Type.MARKET)
                .side(placeOrder.getSide() == Nexus.OrderSide.BID ? Order.Side.BID : Order.Side.ASK)
                .specification(specification)
                .fee(new Order.Fee(placeOrder.getTakerRate(), placeOrder.getMakerRate())) // 0.2% taker, 0.1% maker
                .frozen(new BigDecimal(placeOrder.getFrozen().toString()))
                .build();
    }

    private void setResponseEventType(NexusWrapper one, NexusWrapper other) {
        other.setEventType(one.getEventType());
    }

    private void setSequencerEventType(NexusWrapper one, NexusWrapper other) {
        if (one.isBusinessEvent()) {
            other.setEventType(NexusWrapper.EventType.INTERNAL);
        }
        if (one.isJournalEvent()) {
            other.setEventType(NexusWrapper.EventType.JOURNAL_INTERNAL);
        }
    }
}
