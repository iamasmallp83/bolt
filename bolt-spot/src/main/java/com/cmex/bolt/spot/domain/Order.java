package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

import java.util.Objects;

@Data
@Builder
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

    public enum OrderSide {
        BID, ASK;
    }

    public enum OrderType {
        LIMIT, MARKET;
    }

    public boolean isDone() {
//        if (quantity != null) {
//            return BigDecimalUtil.eqZero(availableQuantity);
//        } else {
//            return BigDecimalUtil.eqZero(availableVolume);
//        }
        return quantity == 0;
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
