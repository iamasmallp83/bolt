package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.Account;
import com.cmex.bolt.spot.domain.Balance;
import com.cmex.bolt.spot.domain.Symbol;
import com.cmex.bolt.spot.repository.impl.AccountRepository;
import com.cmex.bolt.spot.util.Result;
import com.lmax.disruptor.RingBuffer;

import java.util.Optional;

public class AccountService {

    private final AccountRepository repository = new AccountRepository();

    private RingBuffer<Message> responseRingBuffer;
    private RingBuffer<Message> orderRingBuffer;

    public AccountService() {
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        //锁定金额
        int accountId = placeOrder.accountId.get();
        Symbol symbol = Symbol.getSymbol(placeOrder.symbolId.get());
        Optional<Account> optional = repository.get(accountId);
        optional.ifPresentOrElse(account -> {
            //account 存在
            Result<Balance> result;
            if (placeOrder.side.get() == OrderSide.BID) {
                long volume = placeOrder.price.get() * placeOrder.quantity.get();
                result = account.freeze(symbol.getQuote().getId(), volume);
            } else {
                result = account.freeze(symbol.getBase().getId(), placeOrder.quantity.get());
            }
            if (result.isSuccess()) {
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
                    message.payload.asPlaceOrderRejected.reason.set(result.reason());
                });
            }
        }, () -> {
            //account 不存在
            responseRingBuffer.publishEvent((message, sequence) -> {
                message.id.set(messageId);
                message.type.set(EventType.PLACE_ORDER_REJECTED);
                message.payload.asPlaceOrderRejected.reason.set(RejectionReason.ACCOUNT_NOT_EXIST);
            });
        });
    }

    public void on(long messageId, Deposit deposit) {
        int accountId = deposit.accountId.get();
        Account account = repository.putIfAbsent(accountId, new Account(accountId));
        Result<Balance> result = account.deposit(deposit.currencyId.get(), deposit.amount.get());
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.DEPOSITED);
            message.payload.asDeposited.value.set(result.value().getValue());
            message.payload.asDeposited.frozen.set(result.value().getFrozen());
        });
    }

    public void on(long messageId, Withdraw withdraw) {
        int accountId = withdraw.accountId.get();
        Optional<Account> optional = repository.get(accountId);
        Result<Balance> result =
                optional.map(account -> account.withdraw(withdraw.currencyId.get(), withdraw.amount.get()))
                        .orElse(Result.fail(RejectionReason.ACCOUNT_NOT_EXIST));
        responseRingBuffer.publishEvent((message, sequence) -> {
            if (result.isSuccess()) {
                message.id.set(messageId);
                message.type.set(EventType.WITHDRAWN);
                message.payload.asWithdrawn.value.set(result.value().getValue());
                message.payload.asWithdrawn.frozen.set(result.value().getFrozen());
            } else {
                message.id.set(messageId);
                message.type.set(EventType.WITHDRAW_REJECTED);
                message.payload.asWithdrawRejected.reason.set(result.reason());
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
