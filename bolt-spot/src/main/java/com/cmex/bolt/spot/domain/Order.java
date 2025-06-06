package com.cmex.bolt.spot.domain;

import javolution.io.Struct;
import com.cmex.bolt.spot.util.ObjectPool;
import java.util.Objects;

public class Order extends Struct implements ObjectPool.Poolable {
    
    // javolution字段定义
    public final Signed32 symbolId = new Signed32();
    public final Signed64 id = new Signed64();
    public final Signed32 accountId = new Signed32();
    public final Enum32<OrderType> type = new Enum32<>(OrderType.values());
    public final Enum32<OrderSide> side = new Enum32<>(OrderSide.values());
    public final Signed64 price = new Signed64();
    public final Signed64 quantity = new Signed64();
    public final Signed64 volume = new Signed64();
    public final Signed64 availableQuantity = new Signed64();
    public final Signed64 availableVolume = new Signed64();
    public final Signed64 frozen = new Signed64();
    public final Signed64 cost = new Signed64();
    public final Signed32 takerRate = new Signed32();
    public final Signed32 makerRate = new Signed32();
    
    // 运行时Symbol引用（不序列化）
    private transient Symbol symbol;

    public enum OrderSide {
        BID, ASK
    }

    public enum OrderType {
        LIMIT, MARKET
    }
    
    // 实现Poolable接口的reset方法
    @Override
    public Order reset() {
        symbolId.set(0);
        id.set(0);
        accountId.set(0);
        type.set(OrderType.LIMIT);
        side.set(OrderSide.BID);
        price.set(0);
        quantity.set(0);
        volume.set(0);
        availableQuantity.set(0);
        availableVolume.set(0);
        frozen.set(0);
        cost.set(0);
        takerRate.set(0);
        makerRate.set(0);
        symbol = null;
        return this;
    }
    
    // 初始化方法，替代Builder模式
    public Order init(Symbol symbol, long id, int accountId, OrderType type, 
                     OrderSide side, long price, long quantity, long volume, 
                     long frozen, int takerRate, int makerRate) {
        this.symbol = symbol;
        this.symbolId.set(symbol.getId());
        this.id.set(id);
        this.accountId.set(accountId);
        this.type.set(type);
        this.side.set(side);
        this.price.set(price);
        this.quantity.set(quantity);
        this.availableQuantity.set(quantity);
        this.volume.set(volume);
        this.availableVolume.set(volume);
        this.frozen.set(frozen);
        this.cost.set(0);
        this.takerRate.set(takerRate);
        this.makerRate.set(makerRate);
        return this;
    }
    
    // Getter方法
    public Symbol getSymbol() { return symbol; }
    public long getId() { return id.get(); }
    public int getAccountId() { return accountId.get(); }
    public OrderType getType() { return type.get(); }
    public OrderSide getSide() { return side.get(); }
    public long getPrice() { return price.get(); }
    public long getQuantity() { return quantity.get(); }
    public long getVolume() { return volume.get(); }
    public long getAvailableQuantity() { return availableQuantity.get(); }
    public long getAvailableVolume() { return availableVolume.get(); }
    public long getFrozen() { return frozen.get(); }
    public long getCost() { return cost.get(); }
    public int getTakerRate() { return takerRate.get(); }
    public int getMakerRate() { return makerRate.get(); }

    public boolean isDone() {
        if (quantity.get() > 0) {
            return availableQuantity.get() == 0;
        } else {
            return availableVolume.get() == 0;
        }
    }

    public Ticket match(Order maker, ObjectPool<Ticket> ticketPool) {
        long amount;
        long volume;
        if (getType() == OrderType.MARKET) {
            amount = maker.getAvailableQuantity();
            if (getQuantity() > 0) {
                availableQuantity.set(availableQuantity.get() - amount);
            } else {
                volume = symbol.getVolume(maker.getPrice(), amount);
                availableVolume.set(availableVolume.get() - volume);
            }
        } else {
            amount = Math.min(getAvailableQuantity(), maker.getAvailableQuantity());
            availableQuantity.set(availableQuantity.get() - amount);
        }
        volume = symbol.getVolume(maker.getPrice(), amount);
        maker.availableQuantity.set(maker.availableQuantity.get() - amount);
        if (getSide() == OrderSide.BID) {
            cost.set(cost.get() + volume + Rate.getRate(volume, getTakerRate()));
            maker.cost.set(maker.cost.get() + amount + Rate.getRate(amount, maker.getMakerRate()));
        } else {
            cost.set(cost.get() + amount + Rate.getRate(amount, getMakerRate()));
            maker.cost.set(maker.cost.get() + volume + Rate.getRate(volume, maker.getMakerRate()));
        }
        
        // 使用对象池获取Ticket
        return ticketPool.acquire()
                .init(this, maker, maker.getPrice(), amount, volume, this.getSide());
    }

    public Currency getPayCurrency() {
        return symbol.getPayCurrency(getSide());
    }

    public Currency getIncomeCurrency() {
        return symbol.getIncomeCurrency(getSide());
    }

    public long getUnfreezeAmount() {
        if (getSide() == OrderSide.BID) {
            return getAvailableVolume();
        } else {
            return getAvailableQuantity();
        }
    }

    public int getRate(boolean isTaker) {
        return (isTaker ? getTakerRate() : getMakerRate());
    }

    public long left() {
        return getFrozen() - getCost();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Order)) return false;
        Order order = (Order) o;
        return getId() == order.getId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}