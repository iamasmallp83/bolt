package com.cmex.bolt.domain;

import static com.cmex.bolt.Nexus.RejectionReason;

import com.cmex.bolt.util.Result;
import lombok.Data;

import java.math.BigDecimal;
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

    /**
     * 直接添加一个Balance到账户（用于数据恢复）
     */
    public void addBalance(Balance balance) {
        balances.put(balance.getCurrency().getId(), balance);
    }

    public Result<Balance> increase(Currency currency, BigDecimal value) {
        Balance balance = balances.computeIfAbsent(currency.getId(), currencyId -> Balance.builder()
                .currency(currency)
                .value(BigDecimal.ZERO)
                .frozen(BigDecimal.ZERO)
                .build());
        return balance.increase(value);
    }

    public Result<Balance> decrease(int currencyId, BigDecimal value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        return balance.decrease(value);
    }

    public Result<Balance> freeze(int currencyId, BigDecimal value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return Result.fail(RejectionReason.BALANCE_NOT_ENOUGH);
        }
        return balance.freeze(value);
    }

    public Result<Balance> unfreeze(int currencyId, BigDecimal value) {
        Balance balance = balances.get(currencyId);
        return balance.unfreeze(value);
    }

    public Result<Balance> unfreezeAndDecrease(int currencyId, BigDecimal unfreezeAmount, BigDecimal decreaseAmount) {
        Balance balance = balances.get(currencyId);
        return balance.unfreezeAndDecrease(unfreezeAmount, decreaseAmount);
    }

    public void settle(int payCurrencyId, BigDecimal payAmount, BigDecimal refundAmount, Currency incomeCurrency, BigDecimal incomeAmount) {
        this.unfreezeAndDecrease(payCurrencyId, payAmount.add(refundAmount), payAmount);
        this.increase(incomeCurrency, incomeAmount);
    }
}
