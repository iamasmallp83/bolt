package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import it.unimi.dsi.fastutil.booleans.BooleanObjectImmutablePair;
import it.unimi.dsi.fastutil.booleans.BooleanObjectPair;
import lombok.Data;

@Data
public class Balance {
    private long balance;

    private long frozen;

    public BooleanObjectPair<RejectionReason> deposit(long value) {
        this.balance += value;
        return BooleanObjectPair.of(true, null);
    }

    public BooleanObjectImmutablePair<RejectionReason> withdraw(long value) {
        if (this.balance < value) {
            return BooleanObjectImmutablePair.of(false, RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.balance -= value;
        return BooleanObjectImmutablePair.of(true, null);
    }

    public BooleanObjectImmutablePair<RejectionReason> freeze(long value) {
        if (this.balance < value) {
            return BooleanObjectImmutablePair.of(false, RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.frozen += value;
        return BooleanObjectImmutablePair.of(true, null);
    }

    public long available() {
        return balance - frozen;
    }
}
