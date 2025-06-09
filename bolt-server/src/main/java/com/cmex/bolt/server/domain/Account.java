package com.cmex.bolt.server.domain;

import com.cmex.bolt.server.api.RejectionReason;
import com.cmex.bolt.server.util.Result;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Data
public class Account {
    private int id;
    private Map<Integer, Balance> balances = new HashMap<>();

    public Account(int id) {
        this.id = id;
    }

    public Optional<Balance> getBalance(int currencyId) {
        return Optional.ofNullable(balances.get(currencyId));
    }

    public Result<Balance> increase(Currency currency, long value) {
        Balance balance = balances.computeIfAbsent(currency.getId(), currencyId -> Balance.builder()
                .currency(currency)
                .build());
        return balance.increase(value);
    }

    public Result<Balance> decrease(int currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.BALANCE_NOT_EXIST);
        }
        return balance.decrease(value);
    }

    public Result<Balance> freeze(int currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.ACCOUNT_NOT_EXIST);
        }
        return balance.freeze(value);
    }

    public Result<Balance> unfreeze(int currencyId, long value) {
        Balance balance = balances.get(currencyId);
        return balance.unfreeze(value);
    }

    public Result<Balance> unfreezeAndDecrease(int currencyId, long unfreezeAmount, long decreaseAmount) {
        Balance balance = balances.get(currencyId);
        return balance.unfreezeAndDecrease(unfreezeAmount, decreaseAmount);
    }

    public void settle(int payCurrencyId, long payAmount, long refundAmount, Currency incomeCurrency, long incomeAmount) {
        this.unfreezeAndDecrease(payCurrencyId, payAmount + refundAmount, payAmount);
        this.increase(incomeCurrency, incomeAmount);
    }
}
