package com.cmex.bolt.server.api;

import javolution.io.Struct;

public class PlaceOrder extends Struct {
    public final Signed32 symbolId = new Signed32();
    public final Signed32 accountId = new Signed32();
    public final Enum8<OrderType> type = new Enum8<OrderType>(OrderType.values());
    public final Enum8<OrderSide> side = new Enum8<OrderSide>(OrderSide.values());
    public final Signed64 price = new Signed64();
    public final Signed64 quantity = new Signed64();
    public final Signed64 volume = new Signed64();
    public final Signed64 frozen = new Signed64();
    public final Signed32 takerRate = new Signed32();
    public final Signed32 makerRate = new Signed32();

    public void copy(PlaceOrder target) {
        target.symbolId.set(this.symbolId.get());
        target.accountId.set(this.accountId.get());
        target.type.set(this.type.get());
        target.side.set(this.side.get());
        target.price.set(this.price.get());
        target.quantity.set(this.quantity.get());
        target.volume.set(this.volume.get());
        target.frozen.set(this.frozen.get());
        target.takerRate.set(this.takerRate.get());
        target.makerRate.set(this.makerRate.get());
    }

    @Override
    public String toString() {
        return "PlaceOrder{" +
                "symbolId=" + symbolId +
                ", accountId=" + accountId +
                ", type=" + type +
                ", side=" + side +
                ", price=" + price +
                ", quantity=" + quantity +
                ", volume=" + volume +
                ", frozen=" + frozen +
                ", takerRate=" + takerRate +
                ", makerRate=" + makerRate +
                '}';
    }
}