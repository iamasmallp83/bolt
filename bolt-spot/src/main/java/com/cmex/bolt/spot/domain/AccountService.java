package com.cmex.bolt.spot.domain;

import com.cmex.bolt.spot.api.*;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import it.unimi.dsi.fastutil.booleans.BooleanObjectImmutablePair;
import it.unimi.dsi.fastutil.booleans.BooleanObjectPair;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class AccountService {

    private Int2ObjectMap<Account> accounts = new Int2ObjectOpenHashMap<>();

    private RingBuffer<Message> responseRingBuffer;
    private RingBuffer<Message> orderRingBuffer;

    public AccountService() {
    }

    public void on(long messageId, PlaceOrder placeOrder) {
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
        orderRingBuffer.publishEvent((message, sequence) -> {
            message.type.set(EventType.PLACE_ORDER);
            message.payload.asPlaceOrder.symbolId.set(placeOrder.symbolId.get());
            message.payload.asPlaceOrder.accountId.set(placeOrder.accountId.get());
            message.payload.asPlaceOrder.buy.set(placeOrder.buy.get());
            message.payload.asPlaceOrder.price.set(placeOrder.price.get());
            message.payload.asPlaceOrder.size.set(placeOrder.size.get());
        });
    }

    public void on(long messageId, Deposit deposit) {
        int accountId = deposit.accountId.get();
        Account account = accounts.get(accountId);
        if (account == null) {
            account = new Account();
            account.setId(accountId);
            accounts.put(accountId, account);
        }
        account.deposit(deposit.currencyId.get(), deposit.amount.get());
        Balance balance = account.getBalance(deposit.currencyId.get());
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.DEPOSITED);
            message.payload.asDeposited.value.set(balance.getValue());
            message.payload.asDeposited.frozen.set(balance.getFrozen());
        });
    }

    public void on(long messageId, Withdraw withdraw) {
        int accountId = withdraw.accountId.get();
        Account account = accounts.get(accountId);
        BooleanObjectPair<RejectionReason> response;
        if (account == null) {
            response = BooleanObjectImmutablePair.of(false, RejectionReason.ACCOUNT_NOT_EXIST);
        } else {
            response = account.withdraw(withdraw.currencyId.get(), withdraw.amount.get());
        }
        Balance balance = account.getBalance(withdraw.currencyId.get());
        responseRingBuffer.publishEvent((message, sequence) -> {
            if (response.leftBoolean()) {
                message.id.set(messageId);
                message.type.set(EventType.WITHDRAWN);
                message.payload.asWithdrawn.value.set(balance.getValue());
                message.payload.asWithdrawn.frozen.set(balance.getFrozen());
            } else {
                message.id.set(messageId);
                message.type.set(EventType.WITHDRAW_REJECTED);
                message.payload.asWithdrawRejected.reason.set(response.right());
            }
        });
    }

    public void on(long messageId, Unfreeze unfreeze) {
        System.out.println("unfreeze " + unfreeze.accountId.get() + " : " + unfreeze.amount.get());
    }

    public void setOrderRingBuffer(RingBuffer<Message> orderRingBuffer) {
        this.orderRingBuffer = orderRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<Message> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }
}
