package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

import java.util.Objects;

@Data
public class Order {

    private Symbol symbol;

    private long id;

    private int accountId;

    private OrderType type;

    private OrderSide side;

    private long price;

    private long quantity;

    private long volume;

    private long availableQuantity;

    private long availableVolume;

    private long frozen;

    private long cost;

    private int takerRate;

    private int makerRate;

    @Builder
    public Order(Symbol symbol, long id, int accountId, OrderType type, OrderSide side, long price, long quantity,
                 long volume, long frozen, int takerRate, int makerRate) {
        this.symbol = symbol;
        this.id = id;
        this.accountId = accountId;
        this.type = type;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.availableQuantity = this.quantity;
        this.volume = volume;
        this.availableVolume = volume;
        this.frozen = frozen;
        this.takerRate = takerRate;
        this.makerRate = makerRate;
    }

    public enum OrderSide {
        BID, ASK;
    }

    public enum OrderType {
        LIMIT, MARKET;
    }

    public boolean isDone() {
        if (quantity > 0) {
            return availableQuantity == 0;
        } else {
            return availableVolume == 0;
        }
    }

    public Ticket match(Order maker) {
        long amount;
        long volume;
        if (type == OrderType.MARKET) {
            amount = maker.availableQuantity;
            if (quantity > 0) {
                availableQuantity = availableQuantity - amount;
            } else {
                volume = symbol.getVolume(maker.price, amount);
                availableVolume = availableVolume - volume;
            }
        } else {
            amount = Math.min(availableQuantity, maker.availableQuantity);
            availableQuantity = availableQuantity - amount;
        }
        volume = symbol.getVolume(maker.price, amount);
        maker.availableQuantity = maker.availableQuantity - amount;
        if (side == OrderSide.BID) {
            cost += volume + Rate.getRate(volume, takerRate);
            maker.cost += amount + Rate.getRate(amount, makerRate);
        } else {
            cost += amount + Rate.getRate(amount, makerRate);
            maker.cost += volume + Rate.getRate(volume, makerRate);
        }
        return Ticket.builder()
                .taker(this)
                .maker(maker)
                .price(maker.price)
                .quantity(amount)
                .volume(volume)
                .takerSide(this.side)
                .build();
    }

    public Currency getPayCurrency() {
        return symbol.getPayCurrency(side);
    }

    public Currency getIncomeCurrency() {
        return symbol.getIncomeCurrency(side);
    }

    public long getUnfreezeAmount() {
        if (side == OrderSide.BID) {
            return availableVolume;
        } else {
            return availableQuantity;
        }
    }

    public int getRate(boolean isTaker) {
        return (isTaker ? takerRate : makerRate);
    }

    public long left() {
        return frozen - cost;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        Order order = (Order) o;
        return id == order.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
