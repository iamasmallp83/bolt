package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import com.cmex.bolt.spot.util.Result;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Data
public class Account {
    private int id;
    private Map<Short, Balance> balances = new HashMap<>();

    public Account(int id) {
        this.id = id;
    }

    public Optional<Balance> getBalance(short currencyId) {
        return Optional.of(balances.get(currencyId));
    }

    public Result<Balance> increase(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            balance = new Balance();
            balances.put(currencyId, balance);
        }
        return balance.increase(value);
    }

    public Result<Balance> decrease(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.BALANCE_NOT_EXIST);
        }
        return balance.decrease(value);
    }

    public Result<Balance> freeze(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.ACCOUNT_NOT_EXIST);
        }
        return balance.freeze(value);
    }

    public Result<Balance> unfreezeAndDecrease(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        return balance.unfreezeAndDecrease(value);
    }

}
