package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.util.Result;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class OrderBook {

    private final Symbol symbol;

    private final TreeMap<Long, PriceNode> bids;
    private final TreeMap<Long, PriceNode> asks;
    private final Map<Long, Order> orders;


    public OrderBook(Symbol symbol) {
        this.symbol = symbol;
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
        orders = new HashMap<>();
    }

    public Result<List<Ticket>> match(Order taker) {
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
                Ticket ticket = taker.match(maker);
                tickets.add(ticket);
                if (maker.isDone()) {
                    priceNode.remove(maker);
                    orders.remove(maker.getId());
                } else {
                    priceNode.decreaseQuantity(ticket.getQuantity());
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
            orders.put(taker.getId(), taker);
        }
        if (tickets.isEmpty()) {
            return Result.fail(RejectionReason.ORDER_NOT_MATCH);
        }
        return Result.success(tickets);
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
            return taker.getPrice() >= counter.firstKey();
        } else {
            return taker.getPrice() <= counter.firstKey();
        }
    }

    public Result<Order> cancel(long orderId) {
        Order order = orders.get(orderId);
        if (order == null) {
            return Result.fail(RejectionReason.ORDER_NOT_EXIST);
        }
        TreeMap<Long, PriceNode> own = getOwn(order.getSide());
        PriceNode priceNode = own.get(order.getPrice());
        priceNode.remove(order);
        if (priceNode.isDone()) {
            own.remove(priceNode.getPrice());
        }
        return Result.success(order);
    }

    public DepthDto getDepth() {
        return DepthDto.builder()
                .symbol(symbol.getName())
                .bids(convert(bids))
                .asks(convert(asks))
                .build();
    }

    private TreeMap<String, String> convert(TreeMap<Long, PriceNode> target) {
        return target.entrySet().stream().collect(Collectors.toMap(
                entry -> symbol.parsePrice(entry.getKey()),
                entry -> symbol.parseQuantity(entry.getValue().getQuantity()),
                (o1, o2) -> o1,
                TreeMap::new
        ));
    }

    @Override
    public String toString() {
        return "OrderBook{" + "symbol=" + symbol + ", bids=" + bids + ", asks=" + asks + '}';
    }
}
