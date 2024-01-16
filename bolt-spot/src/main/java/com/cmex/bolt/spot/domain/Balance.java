package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.util.Result;
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

    public Result<Balance> deposit(long amount) {
        this.value += amount;
        return Result.success(this);
    }

    public Result<Balance> withdraw(long amount) {
        if (this.value < amount) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.value -= amount;
        return Result.success(this);
    }

    public Result<Balance> freeze(long amount) {
        if (this.value < amount) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.frozen += amount;
        return Result.success(this);
    }

    public long available() {
        return value - frozen;
    }
}
