package com.cmex.bolt.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 重新设计的订单类 - 面向高频交易撮合系统
 * <p>
 * 设计原则：
 * 1. 状态管理清晰 - 区分静态属性和动态状态
 * 2. 撮合优化 - 针对LIMIT/MARKET订单优化匹配逻辑
 * 3. 类型安全 - 使用强类型和值对象
 * 4. 性能优先 - 减少计算和内存分配
 * 5. 数据一致性 - 原子操作和状态验证
 */
@Getter
public final class Order {

    // ==================== 静态属性（订单创建后不变） ====================
    private final long id;                          // 订单唯一标识
    private final int symbolId;                     // 交易对ID
    private final int accountId;                    // 账户ID
    private final Type type;                        // 订单类型
    private final Side side;                        // 买卖方向

    // 订单规格 - 根据订单类型有不同含义
    private final Specification specification;      // 订单规格（价格/数量/金额）
    private final Fee fee;                         // 费率规格

    // ==================== 动态状态（撮合过程中会变化） ====================
    @Setter
    private OrderStatus status;                // 订单状态
    @Setter
    private BigDecimal availableQuantity;      // 可用数量（原子更新）
    @Setter
    private BigDecimal availableAmount;        // 可用金额（原子更新）
    @Setter
    private final BigDecimal frozen;           // 冻结余额
    @Setter
    private BigDecimal cost;                   // 累计成本
    @Setter
    private BigDecimal executedQuantity;       // 已成交数量
    @Setter
    private BigDecimal executedVolume;         // 已成交金额

    // ==================== 订单枚举定义 ====================

    public enum Type {
        LIMIT,      // 限价单：指定价格和数量
        MARKET      // 市价单：指定数量或金额，按市场最优价格成交
    }

    public enum Side {
        BID,        // 买入
        ASK         // 卖出
    }

    public enum OrderStatus {
        NEW,                // 新建
        PARTIALLY_FILLED,   // 部分成交
        FULLY_FILLED,       // 完全成交
        CANCELLED,          // 已取消
        REJECTED           // 已拒绝
    }

    // ==================== 值对象定义 ====================

    /**
     * 订单规格 - 根据订单类型有不同的语义
     */
    @Getter
    public static final class Specification {
        private final BigDecimal price;           // 价格（LIMIT订单必需，MARKET订单为0）
        private final BigDecimal quantity;        // 数量（指定数量的订单）
        private final BigDecimal amount;          // 金额（指定金额的MARKET订单）
        private final QuantityType quantityType;  // 数量类型

        public enum QuantityType {
            BY_QUANTITY,    // 按数量下单（如：买入1个BTC）
            BY_AMOUNT       // 按金额下单（如：用1000USDT买入BTC）
        }

        // 静态工厂方法
        public static Specification limitByQuantity(BigDecimal price, BigDecimal quantity) {
            return new Specification(price, quantity, BigDecimal.ZERO, QuantityType.BY_QUANTITY);
        }

        public static Specification marketByQuantity(BigDecimal quantity) {
            return new Specification(BigDecimal.ZERO, quantity, BigDecimal.ZERO, QuantityType.BY_QUANTITY);
        }

        public static Specification marketByAmount(BigDecimal amount) {
            return new Specification(BigDecimal.ZERO, BigDecimal.ZERO, amount, QuantityType.BY_AMOUNT);
        }

        private Specification(BigDecimal price, BigDecimal quantity, BigDecimal amount, QuantityType quantityType) {
            this.price = price;
            this.quantity = quantity;
            this.amount = amount;
            this.quantityType = quantityType;
        }

        public boolean isPriceSpecified() {
            return price.compareTo(BigDecimal.ZERO) > 0;
        }

        public boolean isQuantityBased() {
            return quantityType == QuantityType.BY_QUANTITY;
        }

        public boolean isAmountBased() {
            return quantityType == QuantityType.BY_AMOUNT;
        }

        @Override
        public String toString() {
            return String.format("%s{price=%s, quantity=%s, amount=%s}",
                    quantityType, price, quantity, amount);
        }
    }

    /**
     * 费率规格
     */
    @Getter
    public static final class Fee {
        private final int taker;     // Taker费率（基点）
        private final int maker;     // Maker费率（基点）

        @Builder
        public Fee(int taker, int maker) {
            this.taker = taker;
            this.maker = maker;
        }

        public long get(boolean isTaker) {
            return isTaker ? taker : maker;
        }

        public long calculateFee(long amount, boolean isTaker) {
            int rateBps = isTaker ? taker : maker;
            return (amount * rateBps) / Rate.BASE_RATE; // 基点转换
        }

        public BigDecimal calculateFeeBD(BigDecimal amount, boolean isTaker) {
            int rateBps = isTaker ? taker : maker;
            return Rate.getRate(amount, rateBps);
        }
    }

    @Builder
    public Order(long id, int symbolId, int accountId, Type type, Side side,
                 Specification specification, Fee fee, BigDecimal frozen) {
        // 静态属性
        this.id = id;
        this.symbolId = symbolId;
        this.accountId = accountId;
        this.type = type;
        this.side = side;
        this.specification = specification;
        this.fee = fee;

        // 初始化动态状态
        this.status = OrderStatus.NEW;
        this.frozen = frozen;
        this.cost = BigDecimal.ZERO;
        this.executedQuantity = BigDecimal.ZERO;
        this.executedVolume = BigDecimal.ZERO;

        // 根据订单类型初始化可用量
        if (specification.isQuantityBased()) {
            this.availableQuantity = specification.getQuantity();
            this.availableAmount = calculateRequiredAmount(specification.getQuantity());
        } else {
            this.availableAmount = specification.getAmount();
            this.availableQuantity = BigDecimal.ZERO; // 将在撮合时计算
        }
    }


    /**
     * @param maker maker订单
     * @return Ticket 成交单据
     */
    public Ticket match(Symbol symbol, Order maker) {
        // 1. 计算匹配结果
        Ticket ticket = calculateMatch(symbol, maker);

        // 2. 应用匹配结果到双方订单
        this.applyMatch(ticket, true);    // this是taker
        maker.applyMatch(ticket, false);  // maker是maker

        return ticket;
    }

    /**
     * 计算与maker订单的匹配结果
     */
    public Ticket calculateMatch(Symbol symbol, Order maker) {
        // 注意：这个方法保留为向后兼容，但实际应该使用BigDecimal版本
        BigDecimal matchPrice = maker.getSpecification().getPrice();
        BigDecimal matchQuantity = determineMatchQuantityBD(maker, matchPrice);
        BigDecimal volume = matchPrice.multiply(matchQuantity);
        return Ticket.builder()
                .id(0)
                .taker(this)
                .maker(maker)
                .price(matchPrice)
                .quantity(matchQuantity)
                .volume(volume)
                .build();
    }

    /**
     * 应用撮合结果（原子操作）(BigDecimal版本)
     */
    public void applyMatch(Ticket ticket, boolean isTaker) {

        BigDecimal quantity = ticket.getQuantity();
        BigDecimal volume = ticket.getVolume();

        // 更新已成交量
        this.executedQuantity = this.executedQuantity.add(quantity);
        this.executedVolume = this.executedVolume.add(volume);

        // 更新可用量
        if (specification.isQuantityBased()) {
            this.availableQuantity = this.availableQuantity.subtract(quantity);
        } else {
            this.availableAmount = this.availableAmount.subtract(volume);
        }

        // 计算并更新成本（包括手续费）
        BigDecimal feeAmount = this.fee.calculateFeeBD(volume, isTaker);
        BigDecimal orderCost = (side == Side.BID) ? volume.add(feeAmount) : quantity.add(feeAmount);
        this.cost = this.cost.add(orderCost);

        // 更新订单状态
        updateStatusBD();
    }

    // ==================== 撤单相关方法 ====================

    /**
     * 计算撤单时需要解冻的金额
     * 这是撤单功能的核心方法
     */
    public BigDecimal calculateUnfreeze() {
        if (side == Side.BID) {
            // 买单撤销：解冻未使用的资金
            return calculateBuyOrderUnfreeze();
        } else {
            // 卖单撤销：解冻未卖出的币
            return calculateSellOrderUnfreeze();
        }
    }

    /**
     * 买单撤销时的解冻计算 (BigDecimal版本)
     * 解冻逻辑：总冻结金额 - 已使用金额 - 剩余订单需要的金额
     */
    private BigDecimal calculateBuyOrderUnfreeze() {
        if (specification.isQuantityBased()) {
            // 按数量买入的限价单/市价单
            if (type == Type.LIMIT) {
                // 限价单：解冻剩余数量对应的金额
                BigDecimal remainingRequiredAmount = availableQuantity.multiply(specification.getPrice());
                // 考虑手续费的预留金额
                BigDecimal remainingFeeReserve = fee.calculateFeeBD(remainingRequiredAmount, true);
                return remainingRequiredAmount.add(remainingFeeReserve);
            } else {
                // 市价买单按数量：解冻剩余的预估金额
                // 市价单的冻结通常是按最坏情况预估的，实际成交后解冻差额
                BigDecimal difference = frozen.subtract(cost);
                return difference.compareTo(BigDecimal.ZERO) > 0 ? difference : BigDecimal.ZERO;
            }
        } else {
            // 按金额买入的市价单：直接解冻剩余金额
            return availableAmount;
        }
    }

    /**
     * 卖单撤销时的解冻计算 (BigDecimal版本)
     * 解冻逻辑：未卖出的币数量
     */
    private BigDecimal calculateSellOrderUnfreeze() {
        if (specification.isQuantityBased()) {
            // 按数量卖出：解冻剩余数量
            return availableQuantity;
        } else {
            // 按金额卖出的市价单：根据当前市价计算剩余数量
            // 这种情况较少，通常需要参考当前市价
            // 这里返回0，实际实现中需要根据当前最优买价计算
            return BigDecimal.ZERO; // 需要外部提供当前市价来计算
        }
    }

    public void cancel() {
        // 更新订单状态
        this.status = OrderStatus.CANCELLED;
    }

    // ==================== 状态查询方法 ====================

    public boolean isDone() {
        return status == OrderStatus.FULLY_FILLED || status == OrderStatus.CANCELLED;
    }

    public boolean isActive() {
        return status == OrderStatus.NEW || status == OrderStatus.PARTIALLY_FILLED;
    }

    public double getFillPercentage() {
        if (specification.isQuantityBased()) {
            return specification.getQuantity().compareTo(BigDecimal.ZERO) > 0 ?
                    executedQuantity.divide(specification.getQuantity(), 18, RoundingMode.HALF_UP).doubleValue() : 0.0;
        } else {
            return specification.getAmount().compareTo(BigDecimal.ZERO) > 0 ?
                    executedVolume.divide(specification.getAmount(), 18, RoundingMode.HALF_UP).doubleValue() : 0.0;
        }
    }

    public BigDecimal getRemainingQuantity() {
        return availableQuantity;
    }

    public BigDecimal getRemainingAmount() {
        return availableAmount;
    }

    /**
     * 获取订单完成时的解冻金额（订单完全成交时）(BigDecimal版本)
     */
    public BigDecimal getUnfreezeAmount() {
        if (status == OrderStatus.FULLY_FILLED) {
            BigDecimal difference = frozen.subtract(cost);
            return difference.compareTo(BigDecimal.ZERO) > 0 ? difference : BigDecimal.ZERO;
        }
        return BigDecimal.ZERO;
    }

    private boolean isPriceMatched(Order maker) {
        if (this.type == Type.MARKET || maker.type == Type.MARKET) {
            return true; // 市价单总是匹配
        }

        // 限价单价格匹配逻辑
        if (this.side == Side.BID) {
            return this.specification.getPrice().compareTo(maker.specification.getPrice()) >= 0;
        } else {
            return this.specification.getPrice().compareTo(maker.specification.getPrice()) <= 0;
        }
    }

    private long determineMatchQuantity(Order maker, long matchPrice) {
        // 注意：这个方法保留为向后兼容，应该使用BigDecimal版本
        BigDecimal thisAvailable = getAvailableQuantityAtPriceBD(new BigDecimal(matchPrice));
        BigDecimal makerAvailable = maker.getAvailableQuantity();
        return thisAvailable.min(makerAvailable).longValue();
    }

    private BigDecimal determineMatchQuantityBD(Order maker, BigDecimal matchPrice) {
        BigDecimal thisAvailable = getAvailableQuantityAtPriceBD(matchPrice);
        BigDecimal makerAvailable = maker.getAvailableQuantity();
        return thisAvailable.min(makerAvailable);
    }

    private long getAvailableQuantityAtPrice(long price) {
        // 注意：这个方法保留为向后兼容，应该使用BigDecimal版本
        return getAvailableQuantityAtPriceBD(new BigDecimal(price)).longValue();
    }

    private BigDecimal getAvailableQuantityAtPriceBD(BigDecimal price) {
        if (specification.isQuantityBased()) {
            return availableQuantity;
        } else {
            // 金额驱动订单：计算在指定价格下能买到的数量
            return price.compareTo(BigDecimal.ZERO) > 0 ?
                    availableAmount.divide(price, 18, RoundingMode.HALF_UP) : BigDecimal.ZERO;
        }
    }

    private BigDecimal calculateRequiredAmount(BigDecimal quantity) {
        if (type == Type.LIMIT) {
            return quantity.multiply(specification.getPrice());
        }
        // MARKET订单的金额在撮合时确定
        return BigDecimal.ZERO;
    }

    private void updateStatus() {
        // 注意：这个方法保留为向后兼容，应该使用BigDecimal版本
        updateStatusBD();
    }

    private void updateStatusBD() {
        if (specification.isQuantityBased()) {
            if (availableQuantity.compareTo(BigDecimal.ZERO) == 0) {
                status = OrderStatus.FULLY_FILLED;
            } else if (executedQuantity.compareTo(BigDecimal.ZERO) > 0) {
                status = OrderStatus.PARTIALLY_FILLED;
            }
        } else {
            if (availableAmount.compareTo(BigDecimal.ZERO) == 0) {
                status = OrderStatus.FULLY_FILLED;
            } else if (executedVolume.compareTo(BigDecimal.ZERO) > 0) {
                status = OrderStatus.PARTIALLY_FILLED;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Order order)) return false;
        return id == order.id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public String toString() {
        return String.format("Order{id=%d, accountId=%d, symbol=%d, %s %s, specification=%s, status=%s, progress=%.2f%%}",
                id, accountId, symbolId, side, type, specification, status, getFillPercentage() * 100);
    }
}