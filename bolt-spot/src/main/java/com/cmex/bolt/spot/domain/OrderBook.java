package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.dto.Depth;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class OrderBook {

    private Symbol symbol;

    protected TreeMap<Long, PriceNode> bids;
    protected TreeMap<Long, PriceNode> asks;

    public OrderBook(Symbol symbol) {
        this.symbol = symbol;
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
    }

    public List<Ticket> match(Order taker) {
        List<Ticket> tickets = new LinkedList<>();
        TreeMap<Long, PriceNode> counter = getCounter(taker.getSide());
        while (tryMatch(counter, taker)) {
            //价格匹配
            boolean takerDone = false;
            Map.Entry<Long, PriceNode> entry = counter.firstEntry();
            PriceNode priceNode = entry.getValue();
            Iterator<Order> it = priceNode.iterator();
            while (it.hasNext()) {
                Order maker = it.next();
                tickets.add(taker.match(maker));
                if (maker.isDone()) {
                    priceNode.remove(maker);
                }
                if (taker.isDone()) {
                    takerDone = true;
                    break;
                }
            }
            if (priceNode.isDone()) {
                counter.remove(priceNode.getPrice());
            }
            if (takerDone) {
                break;
            }
        }
        //价格不匹配
        if (!taker.isDone()) {
            TreeMap<Long, PriceNode> own = getOwn(taker.getSide());
            own.compute(taker.getPrice(), (key, existingValue) -> {
                if (existingValue != null) {
                    existingValue.add(taker);
                    return existingValue;
                } else {
                    return new PriceNode(taker.getPrice(), taker);
                }
            });
        }
        return tickets;
    }

    private TreeMap<Long, PriceNode> getCounter(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? asks : bids;
    }

    private TreeMap<Long, PriceNode> getOwn(Order.OrderSide side) {
        return side == Order.OrderSide.BID ? bids : asks;
    }

    private boolean tryMatch(TreeMap<Long, PriceNode> counter, Order taker) {
        if (counter.isEmpty()) {
            return false;
        }
        if (taker.getSide() == Order.OrderSide.BID) {
            return taker.getPrice() > counter.firstKey();
        } else {
            return taker.getPrice() < counter.firstKey();
        }
    }

    public Depth getDepth() {
        return Depth.builder()
                .symbol(symbol.getName())
                .bids(convert(bids))
                .asks(convert(asks))
                .build();
    }

    private TreeMap<String, String> convert(TreeMap<Long, PriceNode> target) {
        return target.entrySet().stream().collect(Collectors.toMap(
                entry -> String.valueOf(entry.getKey()),
                entry -> String.valueOf(entry.getValue().getQuantity()),
                (o1, o2) -> o1,
                TreeMap::new
        ));
    }

    @Override
    public String toString() {
        return "OrderBook{" + "symbol=" + symbol + ", bids=" + bids + ", asks=" + asks + '}';
    }
}
