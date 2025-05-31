package com.cmex.bolt.spot.api;

public enum RejectionReason {
    CURRENCY_NOT_EXIST(2000),
    ACCOUNT_NOT_EXIST(2001),
    BALANCE_NOT_EXIST(2002),
    BALANCE_NOT_ENOUGH(2003),
    SYMBOL_NOT_EXIST(3000),
    ORDER_NOT_MATCH(3001),
    ORDER_NOT_EXIST(3002),
    SYSTEM_BUSY(1000);

    private int code;

    private RejectionReason(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setMessage(Message message, long messageId, EventType type) {
        message.id.set(messageId);
        message.type.set(type);
        message.payload.asPlaceOrderRejected.reason.set(this);
    }
}
