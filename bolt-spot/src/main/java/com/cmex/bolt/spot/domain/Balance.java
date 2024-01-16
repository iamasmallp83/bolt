package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import it.unimi.dsi.fastutil.booleans.BooleanObjectImmutablePair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Balance {
    private long value;

    private long frozen;

    public BooleanObjectImmutablePair<RejectionReason> deposit(long amount) {
        this.value += amount;
        return BooleanObjectImmutablePair.of(true, null);
    }

    public BooleanObjectImmutablePair<RejectionReason> withdraw(long amount) {
        if (this.value < amount) {
            return BooleanObjectImmutablePair.of(false, RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.value -= amount;
        return BooleanObjectImmutablePair.of(true, null);
    }

    public BooleanObjectImmutablePair<RejectionReason> freeze(long amount) {
        if (this.value < amount) {
            return BooleanObjectImmutablePair.of(false, RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.frozen += amount;
        return BooleanObjectImmutablePair.of(true, null);
    }

    public long available() {
        return value - frozen;
    }
}
