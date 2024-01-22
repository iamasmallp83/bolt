package com.cmex.bolt.spot.domain;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Symbol {
    private int id;

    private String name;

    private Currency base;

    private Currency quote;

    private boolean quoteSettlement;

    public Currency getPayCurrency(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? quote : base;
    }

    public long formatPrice(String price){
        return quote.parse(price);
    }

    public Currency getIncomeCurrency(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? base : quote;
    }

    public long formatQuantity(String quantity){
        return base.parse(quantity);
    }

    public Currency getFeeCurrency(Order.OrderSide side) {
        if (quoteSettlement) {
            return quote;
        }
        return getIncomeCurrency(side);
    }
}
