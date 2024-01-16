package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.util.BigDecimalUtil;
import lombok.Getter;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;

@Getter
public class OrderBook {

    private short symbolId;

    protected TreeMap<BigDecimal, PriceNode> bids;
    protected TreeMap<BigDecimal, PriceNode> asks;

    public OrderBook(short symbolId) {
        this.symbolId = symbolId;
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
    }

    public void match(Order taker) {
        TreeMap<BigDecimal, PriceNode> counter = getCounter(taker.getSide());
        if (tryMatch(counter, taker)) {
            //价格匹配

        } else {
            //价格不配
            TreeMap<BigDecimal, PriceNode> own = getOwn(taker.getSide());
            PriceNode priceNode = own.putIfAbsent(taker.getPrice(), new PriceNode(taker.getPrice(), taker));
            priceNode.add(taker);
        }
    }


    private TreeMap<BigDecimal, PriceNode> getCounter(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? asks : bids;
    }

    private TreeMap<BigDecimal, PriceNode> getOwn(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? bids : asks;
    }

    private boolean tryMatch(TreeMap<BigDecimal, PriceNode> counter, Order taker) {
        if (taker.getSide() == Order.OrderSide.BID) {
            return BigDecimalUtil.gte(taker.getPrice(), counter.firstKey());
        } else {
            return BigDecimalUtil.lte(taker.getPrice(), counter.firstKey());
        }
    }
}
