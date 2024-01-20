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

    @Builder
    public Order(Symbol symbol, long id, int accountId, OrderType type, OrderSide side, long price, long quantity, long volume) {
        this.symbol = symbol;
        this.id = id;
        this.accountId = accountId;
        this.type = type;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.availableQuantity = this.quantity;
        if (volume > 0) {
            this.volume = volume;
            this.availableVolume = volume;
        } else {
            this.volume = price * quantity;
            this.availableVolume = this.volume;
        }
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
        if (type == OrderType.MARKET) {
            amount = maker.availableQuantity;
            if (quantity > 0) {
                availableQuantity = availableQuantity - (amount);
            } else {
                availableVolume = availableVolume - (maker.price * amount);
            }
        } else {
            amount = Math.min(availableQuantity, maker.availableQuantity);
            availableQuantity = availableQuantity - amount;
        }
        maker.availableQuantity = maker.availableQuantity - amount;
        return Ticket.builder()
                .taker(this)
                .maker(maker)
                .price(maker.price)
                .quantity(amount)
                .volume(maker.price * amount)
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
