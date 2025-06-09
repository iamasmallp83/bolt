package com.cmex.bolt.server.domain;

import javolution.io.Struct;
import lombok.Getter;

import java.util.Objects;

public class Order extends Struct {

    // Inner enums (keeping for backward compatibility)
    public enum OrderSide {
        BID, ASK
    }

    public enum OrderType {
        LIMIT, MARKET
    }

    // Javolution Struct fields
    public final Bool allocated = new Bool();
    public final Signed32 symbolId = new Signed32();
    public final Signed64 id = new Signed64();
    public final Signed32 accountId = new Signed32();
    public final Enum8<OrderType> type = new Enum8<>(OrderType.values());
    public final Enum8<OrderSide> side = new Enum8<>(OrderSide.values());
    public final Signed64 price = new Signed64();
    public final Signed64 quantity = new Signed64();
    public final Signed64 volume = new Signed64();
    public final Signed64 availableQuantity = new Signed64();
    public final Signed64 availableVolume = new Signed64();
    public final Signed64 frozen = new Signed64();
    public final Signed64 cost = new Signed64();
    public final Signed32 takerRate = new Signed32();
    public final Signed32 makerRate = new Signed32();

    @Getter
    private transient Symbol symbol;

    public Order() {
        // Default constructor for Javolution
    }

    /**
     * Initialize order with values
     */
    public void init(long id, Symbol symbol, int accountId, OrderType type, OrderSide side, long price, long quantity,
                     long volume, long frozen, int takerRate, int makerRate) {
        this.allocated.set(true);
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
    }

    /**
     * Reset order for object pooling
     */
    public void reset() {
        this.allocated.set(false);
    }

    public void setSymbol(Symbol symbol) {
        this.symbolId.set(symbol.getId());
    }

    public long getId() {
        return id.get();
    }

    public int getAccountId() {
        return accountId.get();
    }

    public OrderType getType() {
        return type.get();
    }

    public OrderSide getSide() {
        return side.get();
    }

    public long getPrice() {
        return price.get();
    }

    public long getQuantity() {
        return quantity.get();
    }

    public long getVolume() {
        return volume.get();
    }

    public long getAvailableQuantity() {
        return availableQuantity.get();
    }

    public void setAvailableQuantity(long value) {
        this.availableQuantity.set(value);
    }

    public long getAvailableVolume() {
        return availableVolume.get();
    }

    public void setAvailableVolume(long value) {
        this.availableVolume.set(value);
    }

    public long getFrozen() {
        return frozen.get();
    }

    public long getCost() {
        return cost.get();
    }

    public void setCost(long value) {
        this.cost.set(value);
    }

    public int getTakerRate() {
        return takerRate.get();
    }

    public int getMakerRate() {
        return makerRate.get();
    }

    public boolean isDone() {
        if (getQuantity() > 0) {
            return getAvailableQuantity() == 0;
        } else {
            return getAvailableVolume() == 0;
        }
    }

    public Ticket match(Order maker) {
        long amount;
        long volume;
        if (getType() == OrderType.MARKET) {
            amount = maker.getAvailableQuantity();
            if (getQuantity() > 0) {
                setAvailableQuantity(getAvailableQuantity() - amount);
            } else {
                volume = symbol.getVolume(maker.getPrice(), amount);
                setAvailableVolume(getAvailableVolume() - volume);
            }
        } else {
            amount = Math.min(getAvailableQuantity(), maker.getAvailableQuantity());
            setAvailableQuantity(getAvailableQuantity() - amount);
        }
        volume = symbol.getVolume(maker.getPrice(), amount);
        maker.setAvailableQuantity(maker.getAvailableQuantity() - amount);

        if (getSide() == OrderSide.BID) {
            setCost(getCost() + volume + Rate.getRate(volume, getTakerRate()));
            maker.setCost(maker.getCost() + amount + Rate.getRate(amount, maker.getMakerRate()));
        } else {
            setCost(getCost() + amount + Rate.getRate(amount, getMakerRate()));
            maker.setCost(maker.getCost() + volume + Rate.getRate(volume, maker.getMakerRate()));
        }

        // Create ticket - need to implement Ticket creation without builder pattern
        Ticket ticket = new Ticket();
        ticket.init(this, maker, maker.getPrice(), amount, volume, this.getSide());
        return ticket;
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

    @Override
    public String toString() {
        return "Order{" +
                "id=" + getId() +
                ", accountId=" + getAccountId() +
                ", type=" + getType() +
                ", side=" + getSide() +
                ", price=" + getPrice() +
                ", quantity=" + getQuantity() +
                ", availableQuantity=" + getAvailableQuantity() +
                '}';
    }
}
