package com.cmex.bolt.spot.domain;

import lombok.Data;

@Data
public class Balance {
    private long balance;

    private long frozen;

    public void deposit(long value) {
        this.balance += value;
    }

    public void freeze(long value) {
        this.frozen += value;
    }

    public long available() {
        return balance - frozen;
    }
}
