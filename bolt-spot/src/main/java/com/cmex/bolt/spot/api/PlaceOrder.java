package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class PlaceOrder extends Struct {
    public final Signed16 symbolId = new Signed16();
    public final Signed32 accountId = new Signed32();
    public final Bool buy = new Bool();
    public final Signed64 price = new Signed64();
    public final Signed64 size = new Signed64();
}
