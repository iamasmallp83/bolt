package com.cmex.bolt.domain;

import lombok.Builder;
import lombok.Data;

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
