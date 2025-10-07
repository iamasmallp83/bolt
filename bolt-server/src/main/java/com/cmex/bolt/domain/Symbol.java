package com.cmex.bolt.domain;

import com.cmex.bolt.dto.DepthDto;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

    private OrderBook orderBook;

    public void init(){
        orderBook = new OrderBook(this);
    }

    public Currency getPayCurrency(Order.Side side) {
        return side == Order.Side.BID ? quote : base;
    }

    public Currency getIncomeCurrency(Order.Side side) {
        return side == Order.Side.BID ? base : quote;
    }

    public Currency getFeeCurrency(Order.Side side) {
        if (quoteSettlement) {
            return quote;
        }
        return getIncomeCurrency(side);
    }

    public DepthDto getDepth() {
        return orderBook.getDepth();
    }
}
