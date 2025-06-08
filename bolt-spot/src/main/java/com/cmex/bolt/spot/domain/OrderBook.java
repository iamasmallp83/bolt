package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.OrderSide;
import com.cmex.bolt.spot.api.OrderType;
import com.cmex.bolt.spot.api.PlaceOrder;
import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.dto.DepthDto;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.cmex.bolt.spot.util.Result;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class OrderBook {
    private final TreeMap<Long, PriceNode> bids;
    private final TreeMap<Long, PriceNode> asks;
    public static int COUNT = 100_000;
    private final Order[] orders = new Order[COUNT];
    private final ByteBuffer buffer;
    private final Queue<Integer> freeIndexes = new LinkedList<>();
    private final Map<Long, Integer> orderId2Index = new HashMap<>(COUNT);
    private final OrderIdGenerator idGenerator = new OrderIdGenerator();

    private final Symbol symbol;

    public OrderBook(Symbol symbol) {
        this.symbol = symbol;
        asks = new TreeMap<>();
        bids = new TreeMap<>(Comparator.reverseOrder());
        Order temp = new Order();
        int size = temp.size();
        buffer = ByteBuffer.allocateDirect(size * COUNT);
        
        // 初始化Order数组和ByteBuffer片段
        for(int i = 0; i < COUNT; i++) {
            Order order = new Order();
            ByteBuffer orderBuffer = createBufferSlice(buffer, i * size, size);
            order.setByteBuffer(orderBuffer, 0);
            orders[i] = order;
            
            // 将所有索引都放入空闲队列
            freeIndexes.offer(i);
        }
    }
    
    /**
     * 创建ByteBuffer的片段，每个片段都是独立的ByteBuffer对象
     * @param sourceBuffer 源ByteBuffer
     * @param offset 起始偏移量
     * @param length 片段长度
     * @return 独立的ByteBuffer片段
     */
    private ByteBuffer createBufferSlice(ByteBuffer sourceBuffer, int offset, int length) {
        // 保存原始position和limit
        int originalPosition = sourceBuffer.position();
        int originalLimit = sourceBuffer.limit();
        
        try {
            // 设置position和limit来定义要slice的区域
            sourceBuffer.position(offset);
            sourceBuffer.limit(offset + length);
            
            // 创建slice - 这会创建一个新的ByteBuffer对象，但共享底层数据
            ByteBuffer slice = sourceBuffer.slice();
            
            // 可选：设置slice的字节序与原buffer一致
            slice.order(sourceBuffer.order());
            
            return slice;
        } finally {
            // 恢复原始的position和limit
            sourceBuffer.position(originalPosition);
            sourceBuffer.limit(originalLimit);
        }
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
        //价格不匹配
        if (taker.isDone()) {
            releaseOrder(taker);
        } else {
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
        Integer index = orderId2Index.get(orderId);
        if (index == null) {
            return Result.fail(RejectionReason.ORDER_NOT_EXIST);
        }
        Order order = orders[index];
        TreeMap<Long, PriceNode> own = getOwn(order.getSide());
        PriceNode priceNode = own.get(order.getPrice());
        priceNode.remove(order);
        if (priceNode.isDone()) {
            own.remove(priceNode.getPrice());
        }
        releaseOrder(order);
        return Result.success(order);
    }

    public Order getOrder(PlaceOrder placeOrder) {
        int index = allocateIndex();
        if (index == -1) {
            throw new RuntimeException("OrderBook pool exhausted for symbol: " + symbol.getName());
        }
        
        Order order = orders[index];
        order.init(
                idGenerator.nextId(symbol.getId()),
                symbol,
                placeOrder.accountId.get(),
                placeOrder.type.get() == OrderType.LIMIT ? Order.OrderType.LIMIT : Order.OrderType.MARKET,
                placeOrder.side.get() == OrderSide.BID ? Order.OrderSide.BID : Order.OrderSide.ASK,
                placeOrder.price.get(),
                placeOrder.quantity.get(),
                placeOrder.volume.get(),
                placeOrder.frozen.get(),
                placeOrder.takerRate.get(),
                placeOrder.makerRate.get()
        );
        orderId2Index.put(order.getId(), index);
        return order;
    }

    private int allocateIndex() {
        // 从空闲索引队列中获取索引
        if (!freeIndexes.isEmpty()) {
            return freeIndexes.poll();
        }
        
        // 池已满，没有可用索引
        return -1;
    }

    private void releaseOrder(Order order) {
        Integer index = orderId2Index.remove(order.getId());
        if (index != null) {
            order.reset();
            freeIndexes.offer(index);
        }
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
     * 获取池的使用统计信息
     */
    public String getPoolStats() {
        int used = orderId2Index.size();
        int available = freeIndexes.size();
        return String.format("OrderPool[%s]: used=%d, available=%d, total=%d, utilization=%.2f%%", 
                symbol.getName(), used, available, COUNT, (used * 100.0 / COUNT));
    }

    @Override
    public String toString() {
        return "OrderBook{" + "symbol=" + symbol.getName() + ", bids=" + bids + ", asks=" + asks + '}';
    }
}
