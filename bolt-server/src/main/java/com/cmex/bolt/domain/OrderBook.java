package com.cmex.bolt.domain;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.util.GenericObjectPool;
import com.cmex.bolt.util.Result;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;
import java.math.BigDecimal;

@Getter
public class OrderBook {

    private final Symbol symbol;

    private final TreeMap<BigDecimal, PriceNode> bids;
    private final TreeMap<BigDecimal, PriceNode> asks;
    private final Map<Long, Order> orders;

    // 缓存最优价格，避免重复TreeMap查找
    private BigDecimal cachedBestBid = null;
    private BigDecimal cachedBestAsk = null;

    // ==================== 预分配内存优化 ====================
    
    // 预分配常量 - 根据实际业务场景调整
    private static final int INITIAL_TICKET_CAPACITY = 50;           // 单次匹配预期的最大成交笔数
    private static final int INITIAL_ORDER_CAPACITY = 5000;          // 订单簿预期容量
    private static final int TICKET_POOL_SIZE = 10;                  // Ticket列表对象池大小
    private static final int MAX_POOLED_LIST_SIZE = 100;             // 池中列表的最大容量限制
    
    // 使用通用对象池替代原有实现
    private final GenericObjectPool<List<Ticket>> ticketListPool;

    public OrderBook(Symbol symbol) {
        this.symbol = symbol;
        
        // 预分配TreeMap和HashMap容量
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
        orders = new HashMap<>(INITIAL_ORDER_CAPACITY); // 预分配订单容量
        
        // 初始化通用对象池
        ticketListPool = new GenericObjectPool<>(
            TICKET_POOL_SIZE,
            () -> new ArrayList<>(INITIAL_TICKET_CAPACITY), // 对象创建工厂
            List::clear, // 重置函数：清空列表
            list -> list instanceof ArrayList, // 验证器：确保是ArrayList类型
            MAX_POOLED_LIST_SIZE // 最大对象大小限制
        );
        
        updateBestPrices(); // 初始化缓存
    }

    /**
     * 从对象池借用Ticket列表
     */
    private List<Ticket> borrowTicketList() {
        return ticketListPool.borrow();
    }

    /**
     * 归还Ticket列表到对象池
     */
    private void returnTicketList(List<Ticket> list) {
        ticketListPool.returnObject(list);
    }

    /**
     * 获取对象池统计信息 - 用于性能监控
     */
    public GenericObjectPool.PoolStatistics getPoolStats() {
        return ticketListPool.getStatistics();
    }

    /**
     * 更新最优价格缓存
     * 只在价格层级发生变化时调用，减少开销
     */
    private void updateBestPrices() {
        cachedBestBid = bids.isEmpty() ? null : bids.firstKey();
        cachedBestAsk = asks.isEmpty() ? null : asks.firstKey();
    }

    /**
     * 智能更新缓存 - 只在新价格可能成为最优价格时更新
     */
    private void updateCacheForNewOrder(Order.Side side, BigDecimal price) {
        if (side == Order.Side.BID) {
            if (cachedBestBid == null || price.compareTo(cachedBestBid) > 0) {
                cachedBestBid = price;
            }
        } else {
            if (cachedBestAsk == null || price.compareTo(cachedBestAsk) < 0) {
                cachedBestAsk = price;
            }
        }
    }

    /**
     * 优化后的tryMatch - 使用缓存价格避免TreeMap查找
     */
    private boolean tryMatch(Order.Side takerSide, Order taker) {
        BigDecimal bestPrice = (takerSide == Order.Side.BID) ? cachedBestAsk : cachedBestBid;
        
        if (bestPrice == null) {
            return false;
        }
        
        if (takerSide == Order.Side.BID) {
            return taker.getSpecification().getPrice().compareTo(bestPrice) >= 0;
        } else {
            return taker.getSpecification().getPrice().compareTo(bestPrice) <= 0;
        }
    }

    public Result<List<Ticket>> match(Order taker) {
        List<Ticket> tickets = borrowTicketList(); // 从对象池借用列表
        
        try {
            TreeMap<BigDecimal, PriceNode> counter = getCounter(taker.getSide());
            
            while (tryMatch(taker.getSide(), taker)) {
                //价格匹配 - 直接使用缓存的价格，避免firstEntry()查找
                BigDecimal bestPrice = (taker.getSide() == Order.Side.BID) ? cachedBestAsk : cachedBestBid;
                PriceNode priceNode = counter.get(bestPrice);
                
                boolean takerDone = false;
                Iterator<Order> it = priceNode.iterator();
                while (it.hasNext()) {
                    Order maker = it.next();
                    Ticket ticket = taker.match(symbol, maker);
                    tickets.add(ticket);
                    if (maker.isDone()) {
                        it.remove(); // 使用iterator的remove方法安全删除
                        orders.remove(maker.getId());
                        priceNode.removeWithoutQuantityUpdate(maker); // 只更新数量，不操作集合
                    } else {
                        priceNode.decreaseQuantity(ticket.getQuantity());
                    }
                    if (taker.isDone()) {
                        takerDone = true;
                        break;
                    }
                }
                if (priceNode.isDone()) {
                    counter.remove(bestPrice);
                    updateBestPrices(); // 只在价格层级删除时更新缓存
                }
                if (takerDone) {
                    break;
                }
            }
            
            //价格不匹配 - 添加新订单到订单簿
            if (!taker.isDone()) {
                BigDecimal price = taker.getSpecification().getPrice();
                TreeMap<BigDecimal, PriceNode> own = getOwn(taker.getSide());
                
                boolean isNewPriceLevel = !own.containsKey(price);
                own.compute(price, (key, existingValue) -> {
                    if (existingValue != null) {
                        existingValue.add(taker);
                        return existingValue;
                    } else {
                        return new PriceNode(price, taker);
                    }
                });
                orders.put(taker.getId(), taker);
                
                // 只在新价格层级时检查是否需要更新缓存
                if (isNewPriceLevel) {
                    updateCacheForNewOrder(taker.getSide(), price);
                }
            }
            
            if (tickets.isEmpty()) {
                return Result.fail(Nexus.RejectionReason.ORDER_NOT_MATCH);
            }
            
            // 创建新的列表返回，避免外部修改池中对象
            // 这里使用ArrayList构造器复制，比手动循环更高效
            return Result.success(new ArrayList<>(tickets));
            
        } finally {
            // 确保在任何情况下都归还对象到池中
            returnTicketList(tickets);
        }
    }

    private TreeMap<BigDecimal, PriceNode> getCounter(Order.Side side) {
        return side == Order.Side.BID ? asks : bids;
    }

    private TreeMap<BigDecimal, PriceNode> getOwn(Order.Side side) {
        return side == Order.Side.BID ? bids : asks;
    }

    public Result<Order> cancel(long orderId) {
        Order order = orders.remove(orderId);
        if (order == null) {
            return Result.fail(Nexus.RejectionReason.ORDER_NOT_EXIST);
        }
        order.cancel();
        TreeMap<BigDecimal, PriceNode> own = getOwn(order.getSide());
        BigDecimal price = order.getSpecification().getPrice();
        PriceNode priceNode = own.get(price);
        priceNode.remove(order);
        
        if (priceNode.isDone()) {
            own.remove(price);
            // 只在最优价格被删除时才需要重新计算缓存
            if ((order.getSide() == Order.Side.BID && price.equals(cachedBestBid)) ||
                (order.getSide() == Order.Side.ASK && price.equals(cachedBestAsk))) {
                updateBestPrices();
            }
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

    private TreeMap<String, String> convert(TreeMap<BigDecimal, PriceNode> target) {
        return target.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().getQuantity().toString(),
                (o1, o2) -> o1,
                TreeMap::new
        ));
    }

    @Override
    public String toString() {
        return "OrderBook{" + "symbol=" + symbol.getName() + ", bids=" + bids + ", asks=" + asks + '}';
    }
}
