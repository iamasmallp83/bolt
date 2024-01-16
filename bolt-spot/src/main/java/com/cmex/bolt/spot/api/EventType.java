package com.cmex.bolt.spot.api;

import javolution.io.Struct;

public enum EventType {
    PLACE_ORDER(new PlaceOrder()),
    ORDER_CREATED(new OrderCreated()),
    PLACE_ORDER_REJECTED(new PlaceOrderRejected()),
    CANCEL_ORDER(new CancelOrder()),
    WITHDRAW(new Withdraw()),
    WITHDRAWN(new Withdrawn()),
    WITHDRAW_REJECTED(new WithdrawRejected()),
    DEPOSIT(new Deposit()),
    DEPOSITED(new Deposited()),
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
