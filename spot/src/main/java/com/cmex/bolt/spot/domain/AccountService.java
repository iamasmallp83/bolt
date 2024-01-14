package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.*;
import com.lmax.disruptor.RingBuffer;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class AccountService {

    private Int2ObjectMap<Account> accounts = new Int2ObjectOpenHashMap<>();

    private RingBuffer<Message> orderRingBuffer;

    public AccountService() {
    }

    public void on(PlaceOrder placeOrder) {
        //锁定金额
        int accountId = placeOrder.accountId.get();
        Account account = accounts.get(accountId);
        Symbol symbol = Symbol.getSymbol(placeOrder.symbolId.get());
        if (placeOrder.buy.get()) {
            long volume = placeOrder.price.get() * placeOrder.size.get();
            account.freeze(symbol.getQuote().getId(), volume);
        } else {
            account.freeze(symbol.getBase().getId(), placeOrder.size.get());
        }
        orderRingBuffer.publishEvent((event, sequence) -> {
            event.type.set(EventType.PLACE_ORDER);
            event.payload.asPlaceOrder.symbolId.set(placeOrder.symbolId.get());
            event.payload.asPlaceOrder.accountId.set(placeOrder.accountId.get());
            event.payload.asPlaceOrder.buy.set(placeOrder.buy.get());
            event.payload.asPlaceOrder.price.set(placeOrder.price.get());
            event.payload.asPlaceOrder.size.set(placeOrder.size.get());
        });
    }

    public void on(Deposit deposit) {
        int accountId = deposit.accountId.get();
        Account account = accounts.get(accountId);
        if (account == null) {
            account = new Account();
            account.setId(accountId);
            accounts.put(accountId, account);
        }
        account.deposit(deposit.currencyId.get(), deposit.amount.get());
    }

    public void on(Withdraw withdraw) {
    }

    public void on(Unfreeze unfreeze) {
        System.out.println("unfreeze " + unfreeze.accountId.get() + " : " + unfreeze.amount.get());
    }

    public void setOrderRingBuffer(RingBuffer<Message> orderRingBuffer) {
        this.orderRingBuffer = orderRingBuffer;
    }
}
