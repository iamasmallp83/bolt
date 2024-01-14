package com.cmex.bolt.spot.domain;

import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import lombok.Data;

@Data
public class Account {
    private int id;
    private Short2ObjectMap<Balance> balances = new Short2ObjectOpenHashMap<>();

    public void deposit(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            balance = new Balance();
            balances.put(currencyId, balance);
        }
        balance.deposit(value);
    }

    public void freeze(short currencyId, long value) {
        Balance balance = balances.get(currencyId);
        if (balance == null) {
            return;
        }
        balance.freeze(value);
    }

}
