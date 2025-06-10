package com.cmex.bolt.domain;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.dto.DepthDto;
import com.cmex.bolt.util.OrderIdGenerator;
import com.cmex.bolt.util.Result;
import javolution.util.FastMap;
import lombok.Getter;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class OrderBook {
    private final TreeMap<Long, PriceNode> bids;
    private final TreeMap<Long, PriceNode> asks;

    // 对象池配置
    public static final int DEFAULT_POOL_SIZE = 100_000;
    private final int poolSize;

    // 内存对齐配置
    private static final int CACHE_LINE_SIZE = 64; // CPU缓存行大小（字节）

    // Order对象数组
    private final Order[] orders;
    // 内存对齐的ByteBuffer
    private final ByteBuffer alignedBuffer;
    // 每个Order的对齐后大小
    private final int alignedOrderSize;

    // BitSet对象池管理
    private final BitSet freeIndexesBitSet;
    private int lastAllocatedIndex = -1;
    private int usedCount = 0;
    private int peakUsedCount = 0;

    // 订单索引映射
    private final FastMap<Long, Integer> orderId2Index = new FastMap<>();
    private final OrderIdGenerator idGenerator = new OrderIdGenerator();
    private final Symbol symbol;

    // 性能统计
    private long totalAllocations = 0;
    private long totalReleases = 0;
    private long cacheHints = 0; // 缓存命中次数

    public OrderBook(Symbol symbol) {
        this(symbol, DEFAULT_POOL_SIZE);
    }

    public OrderBook(Symbol symbol, int poolSize) {
        this.symbol = symbol;
        this.poolSize = poolSize;
        this.asks = new TreeMap<>();
        this.bids = new TreeMap<>(Comparator.reverseOrder());

        // 初始化BitSet，所有位都设为true表示空闲
        this.freeIndexesBitSet = new BitSet(poolSize);
        this.freeIndexesBitSet.set(0, poolSize);

        // 计算内存对齐
        Order tempOrder = new Order();
        int rawOrderSize = tempOrder.size();

        // 对齐到缓存行边界，确保每个Order对象不跨越缓存行
        this.alignedOrderSize = alignToSize(rawOrderSize, CACHE_LINE_SIZE);

        // 分配对齐的内存
        int totalMemoryNeeded = alignedOrderSize * poolSize;
        this.alignedBuffer = allocateAlignedBuffer(totalMemoryNeeded);

        // 初始化Order数组
        this.orders = new Order[poolSize];
        initializeOrderPool();

        System.out.printf("OrderBook[%s] initialized: poolSize=%d, rawOrderSize=%d, alignedOrderSize=%d, totalMemory=%.2fMB%n",
                symbol.getName(), poolSize, rawOrderSize, alignedOrderSize, totalMemoryNeeded / (1024.0 * 1024.0));
    }

    /**
     * 将大小对齐到指定边界
     */
    private int alignToSize(int size, int alignment) {
        return ((size + alignment - 1) / alignment) * alignment;
    }

    /**
     * 分配内存对齐的ByteBuffer
     * 使用简单但有效的对齐策略
     */
    private ByteBuffer allocateAlignedBuffer(int bufferSize) {
        ByteBuffer rawBuffer = ByteBuffer.allocateDirect(bufferSize + CACHE_LINE_SIZE * 2);

        // 从CACHE_LINE_SIZE的倍数位置开始，这样可以保证合理的对齐
        int alignedStart = CACHE_LINE_SIZE;
        rawBuffer.position(alignedStart);
        rawBuffer.limit(alignedStart + bufferSize);
        return rawBuffer.slice();
    }

    /**
     * 获取DirectByteBuffer的内存地址（使用反射，仅用于对齐计算）
     */
    private long getDirectBufferAddress(ByteBuffer buffer) {
        try {
            java.lang.reflect.Field addressField = buffer.getClass().getDeclaredField("address");
            addressField.setAccessible(true);
            return addressField.getLong(buffer);
        } catch (Exception e) {
            // 如果无法获取地址，返回0（将跳过精确对齐）
            System.err.println("Warning: Cannot get buffer address for alignment: " + e.getMessage());
            return 0;
        }
    }

    /**
     * 初始化Order对象池
     */
    private void initializeOrderPool() {
        for (int i = 0; i < poolSize; i++) {
            Order order = new Order();
            ByteBuffer orderBuffer = createAlignedSlice(i);
            order.setByteBuffer(orderBuffer, 0);
            orders[i] = order;
        }
    }

    /**
     * 创建内存对齐的ByteBuffer切片
     */
    private ByteBuffer createAlignedSlice(int index) {
        int offset = index * alignedOrderSize;
        int originalPosition = alignedBuffer.position();
        int originalLimit = alignedBuffer.limit();

        try {
            alignedBuffer.position(offset);
            alignedBuffer.limit(offset + alignedOrderSize);
            ByteBuffer slice = alignedBuffer.slice();
            slice.order(alignedBuffer.order());
            return slice;
        } finally {
            alignedBuffer.position(originalPosition);
            alignedBuffer.limit(originalLimit);
        }
    }

    /**
     * 分配一个空闲索引（优化的局部性算法）
     */
    private int allocateIndex() {
        // 策略1: 尝试在最近分配位置附近查找（提高缓存局部性）
        if (lastAllocatedIndex != -1) {
            int index = findNearbyFreeIndex(lastAllocatedIndex);
            if (index != -1) {
                markIndexAsUsed(index);
                cacheHints++;
                return index;
            }
        }

        // 策略2: 从头开始查找
        int index = freeIndexesBitSet.nextSetBit(0);
        if (index != -1) {
            markIndexAsUsed(index);
            return index;
        }

        // 池已满
        return -1;
    }

    /**
     * 在指定索引附近查找空闲索引（提高缓存局部性）
     */
    private int findNearbyFreeIndex(int centerIndex) {
        // 计算搜索范围（基于缓存行大小）
        int searchRadius = Math.max(8, CACHE_LINE_SIZE / alignedOrderSize);
        int startIndex = Math.max(0, centerIndex - searchRadius);
        int endIndex = Math.min(poolSize - 1, centerIndex + searchRadius);

        // 优先搜索中心点之后的位置
        for (int i = centerIndex + 1; i <= endIndex; i++) {
            if (freeIndexesBitSet.get(i)) {
                return i;
            }
        }

        // 然后搜索中心点之前的位置
        for (int i = centerIndex - 1; i >= startIndex; i--) {
            if (freeIndexesBitSet.get(i)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * 标记索引为已使用
     */
    private void markIndexAsUsed(int index) {
        freeIndexesBitSet.clear(index);
        lastAllocatedIndex = index;
        usedCount++;
        totalAllocations++;

        if (usedCount > peakUsedCount) {
            peakUsedCount = usedCount;
        }
    }

    /**
     * 释放一个索引
     */
    private void releaseIndex(int index) {
        if (index < 0 || index >= poolSize) {
            throw new IllegalArgumentException("Invalid index: " + index);
        }

        freeIndexesBitSet.set(index);
        usedCount--;
        totalReleases++;
    }

    /**
     * 获取可用索引数量
     */
    public int getAvailableCount() {
        return freeIndexesBitSet.cardinality();
    }

    /**
     * 订单匹配核心逻辑
     */
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

        // 处理未完全成交的taker订单
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
            return Result.fail(Nexus.RejectionReason.ORDER_NOT_MATCH);
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

    /**
     * 取消订单
     */
    public Result<Order> cancel(long orderId) {
        Integer index = orderId2Index.get(orderId);
        if (index == null) {
            return Result.fail(Nexus.RejectionReason.ORDER_NOT_EXIST);
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

    /**
     * 创建新订单
     */
    public Order getOrder(Nexus.PlaceOrder placeOrder) {
        int index = allocateIndex();
        if (index == -1) {
            throw new RuntimeException(String.format(
                    "OrderBook pool exhausted for symbol: %s, stats: %s",
                    symbol.getName(), getPoolStats()));
        }

        Order order = orders[index];
        orderId2Index.put(order.getId(), index);
        return order;
    }

    /**
     * 释放订单
     */
    private void releaseOrder(Order order) {
        Integer index = orderId2Index.remove(order.getId());
        if (index != null) {
            order.reset();
            releaseIndex(index);
        }
    }

    /**
     * 获取深度信息
     */
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
     * 获取详细的池统计信息
     */
    public String getPoolStats() {
        double utilization = usedCount * 100.0 / poolSize;
        double cacheHitRate = totalAllocations > 0 ? (cacheHints * 100.0 / totalAllocations) : 0.0;

        return String.format(
                "OrderPool[%s]: used=%d/%d (%.2f%%), peak=%d, cache_hits=%.2f%%, alloc=%d, release=%d, last_idx=%d",
                symbol.getName(), usedCount, poolSize, utilization, peakUsedCount,
                cacheHitRate, totalAllocations, totalReleases, lastAllocatedIndex);
    }

    /**
     * 获取内存对齐信息
     */
    public String getMemoryAlignmentInfo() {
        Order tempOrder = new Order();
        int rawSize = tempOrder.size();

        return String.format(
                "MemoryAlignment[%s]: raw_size=%d, aligned_size=%d, cache_line=%d, padding=%d, total_memory=%.2fMB",
                symbol.getName(), rawSize, alignedOrderSize, CACHE_LINE_SIZE,
                alignedOrderSize - rawSize, (poolSize * alignedOrderSize) / (1024.0 * 1024.0));
    }

    /**
     * 重置池统计信息
     */
    public void resetPoolStats() {
        peakUsedCount = usedCount;
        totalAllocations = 0;
        totalReleases = 0;
        cacheHints = 0;
    }

    /**
     * 池健康检查
     */
    public boolean isPoolHealthy() {
        double utilization = usedCount * 100.0 / poolSize;
        boolean hasLeaks = totalAllocations > 0 && totalReleases > 0 &&
                Math.abs(totalAllocations - totalReleases - usedCount) > 10;

        return utilization < 95.0 && !hasLeaks;
    }

    /**
     * 池性能指标
     */
    public PoolMetrics getPoolMetrics() {
        return PoolMetrics.builder()
                .symbolName(symbol.getName())
                .poolSize(poolSize)
                .usedCount(usedCount)
                .availableCount(getAvailableCount())
                .utilization(usedCount * 100.0 / poolSize)
                .peakUsedCount(peakUsedCount)
                .totalAllocations(totalAllocations)
                .totalReleases(totalReleases)
                .cacheHitRate(totalAllocations > 0 ? (cacheHints * 100.0 / totalAllocations) : 0.0)
                .alignedOrderSize(alignedOrderSize)
                .memoryUsageMB((poolSize * alignedOrderSize) / (1024.0 * 1024.0))
                .healthy(isPoolHealthy())
                .build();
    }

    @Override
    public String toString() {
        return String.format("OrderBook[%s]: bids=%d levels, asks=%d levels, %s",
                symbol.getName(), bids.size(), asks.size(), getPoolStats());
    }

    /**
     * 池性能指标数据类
     */
    public static class PoolMetrics {
        public final String symbolName;
        public final int poolSize;
        public final int usedCount;
        public final int availableCount;
        public final double utilization;
        public final int peakUsedCount;
        public final long totalAllocations;
        public final long totalReleases;
        public final double cacheHitRate;
        public final int alignedOrderSize;
        public final double memoryUsageMB;
        public final boolean healthy;

        private PoolMetrics(Builder builder) {
            this.symbolName = builder.symbolName;
            this.poolSize = builder.poolSize;
            this.usedCount = builder.usedCount;
            this.availableCount = builder.availableCount;
            this.utilization = builder.utilization;
            this.peakUsedCount = builder.peakUsedCount;
            this.totalAllocations = builder.totalAllocations;
            this.totalReleases = builder.totalReleases;
            this.cacheHitRate = builder.cacheHitRate;
            this.alignedOrderSize = builder.alignedOrderSize;
            this.memoryUsageMB = builder.memoryUsageMB;
            this.healthy = builder.healthy;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String symbolName;
            private int poolSize;
            private int usedCount;
            private int availableCount;
            private double utilization;
            private int peakUsedCount;
            private long totalAllocations;
            private long totalReleases;
            private double cacheHitRate;
            private int alignedOrderSize;
            private double memoryUsageMB;
            private boolean healthy;

            public Builder symbolName(String symbolName) {
                this.symbolName = symbolName;
                return this;
            }

            public Builder poolSize(int poolSize) {
                this.poolSize = poolSize;
                return this;
            }

            public Builder usedCount(int usedCount) {
                this.usedCount = usedCount;
                return this;
            }

            public Builder availableCount(int availableCount) {
                this.availableCount = availableCount;
                return this;
            }

            public Builder utilization(double utilization) {
                this.utilization = utilization;
                return this;
            }

            public Builder peakUsedCount(int peakUsedCount) {
                this.peakUsedCount = peakUsedCount;
                return this;
            }

            public Builder totalAllocations(long totalAllocations) {
                this.totalAllocations = totalAllocations;
                return this;
            }

            public Builder totalReleases(long totalReleases) {
                this.totalReleases = totalReleases;
                return this;
            }

            public Builder cacheHitRate(double cacheHitRate) {
                this.cacheHitRate = cacheHitRate;
                return this;
            }

            public Builder alignedOrderSize(int alignedOrderSize) {
                this.alignedOrderSize = alignedOrderSize;
                return this;
            }

            public Builder memoryUsageMB(double memoryUsageMB) {
                this.memoryUsageMB = memoryUsageMB;
                return this;
            }

            public Builder healthy(boolean healthy) {
                this.healthy = healthy;
                return this;
            }

            public PoolMetrics build() {
                return new PoolMetrics(this);
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "PoolMetrics{symbol='%s', used=%d/%d (%.1f%%), peak=%d, cache_hit=%.1f%%, memory=%.1fMB, healthy=%s}",
                    symbolName, usedCount, poolSize, utilization, peakUsedCount, cacheHitRate, memoryUsageMB, healthy);
        }
    }
}