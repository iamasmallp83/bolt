package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.Account;
import com.cmex.bolt.spot.domain.Balance;
import com.cmex.bolt.spot.domain.Symbol;
import com.lmax.disruptor.RingBuffer;
import it.unimi.dsi.fastutil.booleans.BooleanObjectImmutablePair;
import it.unimi.dsi.fastutil.booleans.BooleanObjectPair;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.springframework.stereotype.Service;

@Service
public class AccountService {

    private final Int2ObjectMap<Account> accounts = new Int2ObjectOpenHashMap<>();

    private RingBuffer<Message> responseRingBuffer;
    private RingBuffer<Message> orderRingBuffer;

    public AccountService() {
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //锁定金额
        int accountId = placeOrder.accountId.get();
        Symbol symbol = Symbol.getSymbol(placeOrder.symbolId.get());
        BooleanObjectImmutablePair<RejectionReason> response;
        Account account = accounts.get(accountId);
        if (account == null) {
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.PLACE_ORDER_REJECTED);
                message.payload.asPlaceOrderRejected.reason.set(RejectionReason.ACCOUNT_NOT_EXIST);
            });
            return;
        }
        if (placeOrder.side.get() == OrderSide.BID) {
            long volume = placeOrder.price.get() * placeOrder.size.get();
            response = account.freeze(symbol.getQuote().getId(), volume);
        } else {
            response = account.freeze(symbol.getBase().getId(), placeOrder.size.get());
        }
        if (response.leftBoolean()) {
            orderRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.PLACE_ORDER);
                PlaceOrder payload = message.payload.asPlaceOrder;
                payload.copy(payload);
            });
        } else {
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.PLACE_ORDER_REJECTED);
                message.payload.asPlaceOrderRejected.reason.set(response.right());
            });
        }

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
        final Balance balance;
        if (account == null) {
            response = BooleanObjectImmutablePair.of(false, RejectionReason.ACCOUNT_NOT_EXIST);
        } else {
            response = account.withdraw(withdraw.currencyId.get(), withdraw.amount.get());
        }
        balance = account.getBalance(withdraw.currencyId.get());
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

    public Account getAccount(int accountId) {
        return accounts.get(accountId);
    }
}
