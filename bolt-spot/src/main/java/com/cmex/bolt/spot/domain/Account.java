package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.RejectionReason;
import it.unimi.dsi.fastutil.booleans.BooleanObjectImmutablePair;
import it.unimi.dsi.fastutil.booleans.BooleanObjectPair;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import lombok.Data;

@Data
public class Account {
    private int id;
    private Short2ObjectMap<Balance> balances = new Short2ObjectOpenHashMap<>();

    public BooleanObjectPair<RejectionReason> deposit(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            balance = new Balance();
            balances.put(currencyId, balance);
        }
        return balance.deposit(value);
    }

    public BooleanObjectImmutablePair<RejectionReason> withdraw(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return BooleanObjectImmutablePair.of(true, null);
        }
        return balance.withdraw(value);
    }

    public BooleanObjectPair<RejectionReason> freeze(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return BooleanObjectPair.of(false, RejectionReason.ACCOUNT_NOT_EXIST);
        }
        return balance.freeze(value);
    }

}
