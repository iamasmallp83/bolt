package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public class CancelOrder extends Struct {
    public final Signed64 orderId = new Signed64();
}
