package com.cmex.bolt.domain;

import static com.cmex.bolt.Nexus.RejectionReason;

import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.Result;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
public class Balance {
    private Currency currency;

    private BigDecimal value;

    private BigDecimal frozen;

    public Result<Balance> increase(BigDecimal amount) {
        value = value.add(amount);
        return Result.success(this);
    }

    public Result<Balance> decrease(BigDecimal amount) {
        if (BigDecimalUtil.lt(available(), amount)) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        value = value.subtract(amount);
        return Result.success(this);
    }

    public Result<Balance> freeze(BigDecimal amount) {
        if (BigDecimalUtil.lt(available(), amount)) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        frozen = frozen.add(amount);
        return Result.success(this);
    }

    public Result<Balance> unfreeze(BigDecimal amount) {
        frozen = frozen.subtract(amount);
        return Result.success(this);
    }

    public Result<Balance> unfreezeAndDecrease(BigDecimal unfreezeAmount, BigDecimal decreaseAmount) {
        frozen = frozen.subtract(unfreezeAmount);
        value = value.subtract(decreaseAmount);
        return Result.success(this);
    }

    public BigDecimal available() {
        return value.subtract(frozen);
    }

}
