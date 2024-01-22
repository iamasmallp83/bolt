package com.cmex.bolt.spot.api;

public enum RejectionReason {
    CURRENCY_NOT_EXIST(1000),
    ACCOUNT_NOT_EXIST(1001),
    BALANCE_NOT_EXIST(1002),
    BALANCE_NOT_ENOUGH(1003),
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
