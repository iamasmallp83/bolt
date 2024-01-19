package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public enum EventType {
    PLACE_ORDER(new PlaceOrder()),
    ORDER_CREATED(new OrderCreated()),
    PLACE_ORDER_REJECTED(new PlaceOrderRejected()),
    CANCEL_ORDER(new CancelOrder()),
    DECREASE(new Decrease()),
    DECREASED(new Decreased()),
    DECREASE_REJECTED(new DecreaseRejected()),
    INCREASE(new Increase()),
    INCREASED(new Increased()),
    FREEZE(new Freeze()),
    UNFREEZE(new Unfreeze()),
    CLEARED(new Cleared());

    private Struct struct;

    private EventType(Struct struct) {
        this.struct = struct;
    }

    public Struct getStruct() {
        return struct;
    }
}
