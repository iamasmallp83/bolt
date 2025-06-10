package com.cmex.bolt.domain;

import static com.cmex.bolt.Nexus.RejectionReason;

import com.cmex.bolt.util.Result;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Balance {
    private Currency currency;

    private long value;

    private long frozen;

    public Result<Balance> increase(long amount) {
        this.value += amount;
        return Result.success(this);
    }

    public Result<Balance> decrease(long amount) {
        if (this.available() < amount) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.value -= amount;
        return Result.success(this);
    }

    public Result<Balance> freeze(long amount) {
        if (this.available() < amount) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        this.frozen += amount;
        return Result.success(this);
    }

    public Result<Balance> unfreeze(long amount) {
        frozen -= amount;
        return Result.success(this);
    }

    public Result<Balance> unfreezeAndDecrease(long unfreezeAmount, long decreaseAmount) {
        frozen -= unfreezeAmount;
        value -= decreaseAmount;
        return Result.success(this);
    }

    public long available() {
        return value - frozen;
    }

    public String getFormatValue() {
        return currency.format(value);
    }

    public String getFormatFrozen() {
        return currency.format(frozen);
    }


    public String getFormatAvailable() {
        return currency.format(available());
    }
}
