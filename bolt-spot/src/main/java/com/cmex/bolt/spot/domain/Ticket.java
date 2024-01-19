package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class Ticket {

    private long id;

    private Order taker;

    private Order maker;

    private long price;

    private long quantity;

    private long volume;

    private Order.OrderSide takerSide;

}