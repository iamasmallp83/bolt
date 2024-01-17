package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.util.BigDecimalUtil;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Objects;

@Data
@Builder
public class Order {

    private long id;

    private int accountId;

    private OrderType type;

    private OrderSide side;

    private BigDecimal price;

    private BigDecimal quantity;

    private BigDecimal volume;

    private BigDecimal availableQuantity;

    private BigDecimal availableVolume;

    public enum OrderSide {
        BID, ASK;
    }

    public enum OrderType {
        LIMIT, MARKET;
    }

    public boolean isDone() {
        if (quantity != null) {
            return BigDecimalUtil.eqZero(availableQuantity);
        } else {
            return BigDecimalUtil.eqZero(availableVolume);
        }
    }

    public Ticket match(Order maker) {
        BigDecimal amount;
        if (type == OrderType.MARKET) {
            amount = maker.availableQuantity;
            if (quantity != null) {
                availableQuantity = availableQuantity.subtract(amount);
            } else {
                availableVolume = availableVolume.subtract(maker.price.multiply(amount));
            }
        } else {
            amount = BigDecimalUtil.min(availableQuantity, maker.availableQuantity);
            availableQuantity = availableQuantity.subtract(amount);
        }
        maker.availableQuantity = maker.availableQuantity.subtract(amount);
        return Ticket.builder()
                .taker(this)
                .maker(maker)
                .price(maker.price)
                .quantity(amount)
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
