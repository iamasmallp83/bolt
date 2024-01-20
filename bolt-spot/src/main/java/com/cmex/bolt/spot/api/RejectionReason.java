package com.cmex.bolt.spot.api;

public enum RejectionReason {
    ACCOUNT_NOT_EXIST(1000),
    BALANCE_NOT_EXIST(1001),
    BALANCE_NOT_ENOUGH(1002),
    SYMBOL_NOT_EXIST(2000),
    ORDER_NOT_MATCH(2001),
    ORDER_NOT_EXIST(2002);

    private int code;

    private RejectionReason(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
