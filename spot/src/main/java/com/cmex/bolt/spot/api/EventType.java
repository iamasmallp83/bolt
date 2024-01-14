package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public enum EventType {
    PLACE_ORDER(new PlaceOrder()),
    CANCEL_ORDER(new CancelOrder()),
    WITHDRAW(new Withdraw()),
    DEPOSIT(new Deposit()),
    FREEZE(new Freeze()),
    UNFREEZE(new Unfreeze());
    private Struct struct;

    private EventType(Struct struct) {
        this.struct = struct;
    }

    public Struct getStruct() {
        return struct;
    }
}
