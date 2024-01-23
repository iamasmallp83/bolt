package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.*;
import com.cmex.bolt.spot.domain.*;
import com.cmex.bolt.spot.repository.impl.AccountRepository;
import com.cmex.bolt.spot.repository.impl.CurrencyRepository;
import com.cmex.bolt.spot.repository.impl.SymbolRepository;
import com.cmex.bolt.spot.util.Result;
import com.lmax.disruptor.RingBuffer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AccountService {

    private final AccountRepository accountRepository;
    private final CurrencyRepository currencyRepository;

    private final SymbolRepository symbolRepository;

    private RingBuffer<Message> responseRingBuffer;
    private RingBuffer<Message> matchRingBuffer;

    public AccountService() {
        this.accountRepository = new AccountRepository();
        this.currencyRepository = new CurrencyRepository();
        this.symbolRepository = new SymbolRepository();
    }

    public Optional<Currency> getCurrency(int currencyId) {
        return currencyRepository.get(currencyId);
    }

    public Map<Integer, Balance> getBalances(int accountId, int currencyId) {
        return accountRepository.get(accountId)
                .map(account -> currencyId == 0 ? account.getBalances() :
                        account.getBalance(currencyId).map(balance -> Map.of(currencyId, balance))
                                .orElse(Collections.emptyMap()))
                .orElse(Collections.emptyMap());
    }

    public void on(long messageId, PlaceOrder placeOrder) {
        int symbolId = placeOrder.symbolId.get();
        symbolRepository.get(symbolId).ifPresentOrElse(
                symbol -> handleSymbolPresent(messageId, symbol, placeOrder),
                () -> handleSymbolAbsent(messageId, EventType.PLACE_ORDER_REJECTED)
        );
    }

    private void handleSymbolPresent(long messageId, Symbol symbol, PlaceOrder placeOrder) {
        accountRepository.get(placeOrder.accountId.get()).ifPresentOrElse(
                account -> handleAccountPresent(messageId, symbol, account, placeOrder),
                () -> handleAccountAbsent(messageId, EventType.PLACE_ORDER_REJECTED)
        );
    }

    private void handleSymbolAbsent(long messageId, EventType eventType) {
        publishFailureEvent(messageId, eventType, RejectionReason.SYMBOL_NOT_EXIST);
    }

    private void handleAccountPresent(long messageId, Symbol symbol, Account account, PlaceOrder placeOrder) {
        Result<Balance> result = calculateAndFreezeAmount(symbol, account, placeOrder);
        if (result.isSuccess()) {
            publishPlaceOrderEvent(messageId, placeOrder);
        } else {
            publishFailureEvent(messageId, EventType.PLACE_ORDER_REJECTED, result.reason());
        }
    }

    private void handleAccountAbsent(long messageId, EventType eventType) {
        publishFailureEvent(messageId, eventType, RejectionReason.ACCOUNT_NOT_EXIST);
    }

    private void publishPlaceOrderEvent(long messageId, PlaceOrder placeOrder) {
        matchRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.PLACE_ORDER);
            PlaceOrder payload = message.payload.asPlaceOrder;
            placeOrder.copy(payload);
        });
    }

    public void on(long messageId, Increase increase) {
        int accountId = increase.accountId.get();
        Account account = accountRepository.getOrCreate(accountId, new Account(accountId));
        currencyRepository.get(increase.currencyId.get()).ifPresentOrElse(
                currency -> handleCurrencyPresent(messageId, increase, account, currency),
                () -> handleCurrencyAbsent(messageId)
        );
    }

    public void on(long messageId, Decrease decrease) {
        int accountId = decrease.accountId.get();
        Result<Balance> result = accountRepository.get(accountId)
                .map(account -> account.decrease(decrease.currencyId.get(), decrease.amount.get()))
                .orElse(Result.fail(RejectionReason.ACCOUNT_NOT_EXIST));
        if (result.isSuccess()) {
            publishDecreasedEvent(messageId, result.value());
        } else {
            publishFailureEvent(messageId, EventType.DECREASE_REJECTED, result.reason());
        }
    }

    private void publishIncreasedEvent(long messageId, Balance balance) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.INCREASED);
            Increased increased = message.payload.asIncreased;
            increased.currency.set(balance.getCurrency().getName());
            increased.value.set(balance.getFormatValue());
            increased.frozen.set(balance.getFormatFrozen());
            increased.available.set(balance.getFormatAvailable());
        });
    }

    private void publishDecreasedEvent(long messageId, Balance balance) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.id.set(messageId);
            message.type.set(EventType.DECREASED);
            Decreased decreased = message.payload.asDecreased;
            decreased.currency.set(balance.getCurrency().getName());
            decreased.value.set(balance.getFormatValue());
            decreased.frozen.set(balance.getFormatFrozen());
            decreased.available.set(balance.getFormatAvailable());
        });
    }

    private Result<Balance> calculateAndFreezeAmount(Symbol symbol, Account account, PlaceOrder placeOrder) {
        if (placeOrder.side.get() == OrderSide.BID) {
            long volume;
            if (placeOrder.volume.get() > 0) {
                volume = placeOrder.volume.get();
            } else {
                volume = symbol.getVolume(placeOrder.price.get(), placeOrder.quantity.get());
            }
            if (symbol.isQuoteSettlement()) {
                volume += Rate.getRate(volume, placeOrder.takerRate.get());
            }
            placeOrder.locked.set(volume);
            return account.freeze(symbol.getQuote().getId(), volume);
        } else {
            placeOrder.locked.set(placeOrder.quantity.get());
            return account.freeze(symbol.getBase().getId(), placeOrder.quantity.get());
        }
    }

    private void handleCurrencyPresent(long messageId, Increase increase, Account account, Currency currency) {
        Result<Balance> result = account.increase(currency, increase.amount.get());
        publishIncreasedEvent(messageId, result.value());
    }

    private void handleCurrencyAbsent(long messageId) {
        publishFailureEvent(messageId, EventType.INCREASE_REJECTED, RejectionReason.CURRENCY_NOT_EXIST);
    }

    private void publishFailureEvent(long messageId, EventType eventType, RejectionReason rejectionReason) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            rejectionReason.setMessage(message, messageId, eventType);
        });
    }


    public void on(long messageId, Unfreeze unfreeze) {
        int accountId = unfreeze.accountId.get();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.unfreeze(unfreeze.currencyId.get(), unfreeze.amount.get());
    }

    public void on(long messageId, Cleared cleared) {
        int accountId = cleared.accountId.get();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.settle(cleared.payCurrencyId.get(), cleared.payAmount.get(), cleared.refundAmount.get(),
                currencyRepository.get(cleared.incomeCurrencyId.get()).get(), cleared.incomeAmount.get());
    }

    public void setMatchRingBuffer(RingBuffer<Message> matchRingBuffer) {
        this.matchRingBuffer = matchRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<Message> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }

}
