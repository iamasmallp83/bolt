package com.cmex.bolt.service;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.util.Result;
import com.lmax.disruptor.RingBuffer;
import lombok.Setter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AccountService {

    private final int group;
    private final AccountRepository accountRepository;
    private final CurrencyRepository currencyRepository;

    private final SymbolRepository symbolRepository;

    @Setter
    private RingBuffer<NexusWrapper> responseRingBuffer;
    @Setter
    private RingBuffer<NexusWrapper> matchRingBuffer;
    private final Transfer transfer = new Transfer();

    public AccountService(int group) {
        this.group = group;
        this.accountRepository = new AccountRepository();
        this.currencyRepository = CurrencyRepository.getInstance();
        this.symbolRepository = SymbolRepository.getInstance();
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

    public void on(long messageId, Nexus.PlaceOrder.Reader placeOrder) {
        int symbolId = placeOrder.getSymbolId();
        symbolRepository.get(symbolId).ifPresentOrElse(
                symbol -> handleSymbolPresent(messageId, symbol, placeOrder),
                () -> handleSymbolAbsent(messageId)
        );
    }

    private void handleSymbolPresent(long messageId, Symbol symbol, Nexus.PlaceOrder.Reader placeOrder) {
        int accountId = placeOrder.getAccountId();
        accountRepository.get(accountId).ifPresentOrElse(
                account -> handleAccountPresent(messageId, symbol, account, placeOrder),
                () -> handleAccountAbsent(messageId)
        );
    }

    private void handleSymbolAbsent(long messageId) {
        publishFailureEvent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.SYMBOL_NOT_EXIST);
    }

    private void handleAccountPresent(long messageId, Symbol symbol, Account account, Nexus.PlaceOrder.Reader placeOrder) {
        Result<Long> result = calculateAndFreezeAmount(symbol, account, placeOrder);
        if (result.isSuccess()) {
            publishPlaceOrderEvent(messageId, placeOrder);
        } else {
            publishFailureEvent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED, result.reason());
        }
    }

    private void handleAccountAbsent(long messageId) {
        publishFailureEvent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.BALANCE_NOT_ENOUGH);
    }

    private void publishPlaceOrderEvent(long messageId, Nexus.PlaceOrder.Reader placeOrder) {
        matchRingBuffer.publishEvent((wrapper, sequence) -> {
            wrapper.setId(messageId);
            wrapper.setPartition(placeOrder.getSymbolId() % group);
            transfer.writePlaceOrder(placeOrder, wrapper.getBuffer());
        });
    }

    public void on(long messageId, Nexus.Increase.Reader increase) {
        int accountId = increase.getAccountId();
        int currencyId = increase.getCurrencyId();
        Account account = accountRepository.getOrCreate(accountId, new Account(accountId));
        //前置已经检查币种存在
        currencyRepository.get(currencyId).ifPresent(
                currency -> doIncrease(messageId, increase, account, currency));
    }

    public void on(long messageId, Nexus.Decrease.Reader decrease) {
        int accountId = decrease.getAccountId();
        Result<Balance> result = accountRepository.get(accountId)
                .map(account -> account.decrease(decrease.getCurrencyId(), decrease.getAmount()))
                .orElse(Result.fail(Nexus.RejectionReason.BALANCE_NOT_ENOUGH));
        if (result.isSuccess()) {
            publishDecreasedEvent(messageId, result.value());
        } else {
            publishFailureEvent(messageId, Nexus.EventType.DECREASE_REJECTED, result.reason());
        }
    }

    private void publishIncreasedEvent(long messageId, Balance balance) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.setId(messageId);
            transfer.writeBalance(balance, Nexus.EventType.INCREASED, message.getBuffer());
        });
    }

    private void publishDecreasedEvent(long messageId, Balance balance) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.setId(messageId);
            transfer.writeBalance(balance, Nexus.EventType.DECREASED, message.getBuffer());
        });
    }

    private Result<Long> calculateAndFreezeAmount(Symbol symbol, Account account, Nexus.PlaceOrder.Reader placeOrder) {
        Result<Balance> freezeResult;
        long amount = 0;
        if (placeOrder.getSide() == Nexus.OrderSide.BID) {
            if (placeOrder.getVolume() > 0) {
                amount = placeOrder.getVolume();
            } else {
                amount = symbol.getVolume(placeOrder.getPrice(), placeOrder.getQuantity());
            }
            if (symbol.isQuoteSettlement()) {
                amount += Rate.getRate(amount, placeOrder.getTakerRate());
            }
            freezeResult = account.freeze(symbol.getQuote().getId(), amount);
        } else {
            freezeResult = account.freeze(symbol.getBase().getId(), placeOrder.getQuantity());
        }
        if (freezeResult.isSuccess()) {
            return Result.success(amount);
        } else {
            return Result.fail(freezeResult.reason());
        }
    }

    private void doIncrease(long messageId, Nexus.Increase.Reader increase, Account account, Currency currency) {
        Result<Balance> result = account.increase(currency, increase.getAmount());//increase.amount.get());
        publishIncreasedEvent(messageId, result.value());
    }

    private void publishFailureEvent(long messageId, Nexus.EventType eventType, Nexus.RejectionReason rejectionReason) {
        responseRingBuffer.publishEvent((wrapper, sequence) -> {
            wrapper.setId(messageId);
            transfer.writeFailed(eventType, rejectionReason, wrapper.getBuffer());
        });
    }


    public void on(Nexus.Unfreeze.Reader unfreeze) {
        int accountId = unfreeze.getAccountId();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.unfreeze(unfreeze.getCurrencyId(), unfreeze.getAmount());
    }

    public void on(Nexus.Clear.Reader clear) {
        int accountId = clear.getAccountId();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.settle(clear.getPayCurrencyId(), clear.getPayAmount(), clear.getRefundAmount(),
                currencyRepository.get(clear.getIncomeCurrencyId()).get(), clear.getIncomeAmount());
    }

}
