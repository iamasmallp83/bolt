package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class Order {

    private long id;

    private int accountId;

    private OrderSide side;

    private BigDecimal price;

    private BigDecimal size;

    public enum OrderSide {
        BID, ASK;
    }
}
