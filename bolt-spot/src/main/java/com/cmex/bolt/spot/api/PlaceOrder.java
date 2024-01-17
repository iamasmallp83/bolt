package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class PlaceOrder extends Struct {
    public final Signed16 symbolId = new Signed16();
    public final Signed32 accountId = new Signed32();
    public final Enum8<OrderType> type = new Enum8<OrderType>(OrderType.values());
    public final Enum8<OrderSide> side = new Enum8<OrderSide>(OrderSide.values());
    public final Signed64 price = new Signed64();
    public final Signed64 quantity = new Signed64();
    public final Signed64 volume = new Signed64();

    public void copy(PlaceOrder target) {
        target.symbolId.set(this.symbolId.get());
        target.accountId.set(this.accountId.get());
        target.type.set(this.type.get());
        target.side.set(this.side.get());
        target.price.set(this.price.get());
        target.quantity.set(this.quantity.get());
        target.volume.set(this.volume.get());
    }
}