package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.util.Result;
import com.cmex.bolt.spot.util.ObjectPool;
import com.cmex.bolt.spot.util.OrderPool;
import com.cmex.bolt.spot.util.TicketPool;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class OrderBook {

    private final Symbol symbol;
    private final TreeMap<Long, PriceNode> bids;
    private final TreeMap<Long, PriceNode> asks;
    private final Map<Long, Order> orders;
    
    // 对象池
    private final OrderPool orderPool;
    private final TicketPool ticketPool;

    public OrderBook(Symbol symbol) {
        
        this.symbol = symbol;
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
        orders = new HashMap<>();
        
        // 初始化对象池 - 使用懒加载策略，不预分配对象，节省启动内存
        this.orderPool = new OrderPool(100000, true);   // 懒加载Order池
        this.ticketPool = new TicketPool(10000, true); // 懒加载Ticket池
    }
    
    // 获取Order对象，优先从池中获取
    public Order acquireOrder() {
        return orderPool.acquire();
    }
    
    // 获取并初始化Order对象
    public Order acquireAndInitOrder(long id, int accountId, Order.OrderType type, 
                                    Order.OrderSide side, long price, long quantity, 
                                    long volume, long frozen, int takerRate, int makerRate) {
        return orderPool.acquireAndInit(symbol, id, accountId, type, side, price, 
                                       quantity, volume, frozen, takerRate, makerRate);
    }
    
    // 释放Order对象回池
    public void releaseOrder(Order order) {
        orderPool.release(order);
    }
    
    // 获取Ticket对象
    public Ticket acquireTicket() {
        return ticketPool.acquire();
    }
    
    // 释放Ticket对象回池
    public void releaseTicket(Ticket ticket) {
        ticketPool.release(ticket);
    }
    
    // 批量释放Ticket对象
    public void releaseTickets(List<Ticket> tickets) {
        ticketPool.releaseAll(tickets);
    }

    public Result<List<Ticket>> match(Order taker) {
        List<Ticket> tickets = new LinkedList<>();
        TreeMap<Long, PriceNode> counter = getCounter(taker.getSide());
        
        while (tryMatch(counter, taker)) {
            boolean takerDone = false;
            Map.Entry<Long, PriceNode> entry = counter.firstEntry();
            PriceNode priceNode = entry.getValue();
            Iterator<Order> it = priceNode.iterator();
            
            while (it.hasNext()) {
                Order maker = it.next();
                Ticket ticket = taker.match(maker, ticketPool); // 使用对象池
                tickets.add(ticket);
                
                if (maker.isDone()) {
                    priceNode.remove(maker);
                    orders.remove(maker.getId());
                    // 释放已完成的Order回池
                    releaseOrder(maker);
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
        
        // 如果taker未完成，加入orderbook
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
        orders.remove(orderId);
        
        if (priceNode.isDone()) {
            own.remove(priceNode.getPrice());
        }
        
        // 释放取消的Order回池
        releaseOrder(order);
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
    
    /**
     * 获取对象池统计信息
     */
    public ObjectPool.PoolStats getOrderPoolStats() {
        return orderPool.getStats();
    }
    
    public ObjectPool.PoolStats getTicketPoolStats() {
        return ticketPool.getStats();
    }
    
    /**
     * 重置对象池统计信息
     */
    public void resetPoolStats() {
        orderPool.resetStats();
        ticketPool.resetStats();
    }

    @Override
    public String toString() {
        return "OrderBook{" + "symbol=" + symbol + ", bids=" + bids + ", asks=" + asks + 
               ", orderPool=" + orderPool.getStats() + ", ticketPool=" + ticketPool.getStats() + '}';
    }
}
