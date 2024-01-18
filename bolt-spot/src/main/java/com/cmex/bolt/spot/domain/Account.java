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

    public synchronized Result<Balance> deposit(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            balance = new Balance();
            balances.put(currencyId, balance);
        }
        return balance.deposit(value);
    }

    public synchronized Result<Balance> withdraw(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.BALANCE_NOT_EXIST);
        }
        return balance.withdraw(value);
    }

    public synchronized Result<Balance> freeze(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.ACCOUNT_NOT_EXIST);
        }
        return balance.freeze(value);
    }

}
