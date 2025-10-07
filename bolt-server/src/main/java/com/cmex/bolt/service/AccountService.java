package com.cmex.bolt.service;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.util.BigDecimalUtil;
import com.cmex.bolt.util.Result;
import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AccountService {

    private final int group;

    @Getter
    private final AccountRepository accountRepository;

    @Getter
    private final CurrencyRepository currencyRepository;

    @Getter
    private final SymbolRepository symbolRepository;

    @Setter
    private RingBuffer<NexusWrapper> responseRingBuffer;
    @Setter
    private RingBuffer<NexusWrapper> matchingRingBuffer;
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

    public void on(NexusWrapper wrapper, Nexus.PlaceOrder.Reader placeOrder) {
        int symbolId = placeOrder.getSymbolId();
        symbolRepository.get(symbolId).ifPresentOrElse(
                symbol -> handleSymbolPresent(wrapper, symbol, placeOrder),
                () -> handleSymbolAbsent(wrapper)
        );
    }

    private void handleSymbolPresent(NexusWrapper wrapper, Symbol symbol, Nexus.PlaceOrder.Reader placeOrder) {
        int accountId = placeOrder.getAccountId();
        accountRepository.get(accountId).ifPresentOrElse(
                account -> handleAccountPresent(wrapper, symbol, account, placeOrder),
                () -> handleAccountAbsent(wrapper)
        );
    }

    private void handleSymbolAbsent(NexusWrapper wrapper) {
        if (wrapper.isJournalEvent()) {
            return;
        }
        publishFailureEvent(wrapper, Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.SYMBOL_NOT_EXIST);
    }

    private void handleAccountPresent(NexusWrapper wrapper, Symbol symbol, Account account, Nexus.PlaceOrder.Reader placeOrder) {
        Currency currency = placeOrder.getSide() == Nexus.OrderSide.BID ? symbol.getQuote() : symbol.getBase();
        Result<Balance> result = account.freeze(currency.getId(), new BigDecimal(placeOrder.getFrozen().toString()));
        if (result.isSuccess()) {
            publishPlaceOrderEvent(wrapper, placeOrder);
        } else {
            publishFailureEvent(wrapper, Nexus.EventType.PLACE_ORDER_REJECTED, result.reason());
        }
    }

    private void handleAccountAbsent(NexusWrapper wrapper) {
        if (wrapper.isJournalEvent()) {
            return;
        }
        publishFailureEvent(wrapper, Nexus.EventType.PLACE_ORDER_REJECTED, Nexus.RejectionReason.BALANCE_NOT_ENOUGH);
    }

    private void publishPlaceOrderEvent(NexusWrapper wrapper, Nexus.PlaceOrder.Reader placeOrder) {
        matchingRingBuffer.publishEvent((matchingWrapper, sequence) -> {
            matchingWrapper.setId(wrapper.getId());
            matchingWrapper.setEventType(wrapper.getEventType());
            matchingWrapper.setPartition(placeOrder.getSymbolId() % group);
            transfer.writePlaceOrder(placeOrder, matchingWrapper.getBuffer());
        });
    }

    public void on(NexusWrapper wrapper, Nexus.Increase.Reader increase) {
        int accountId = increase.getAccountId();
        int currencyId = increase.getCurrencyId();
        Account account = accountRepository.getOrCreate(accountId, new Account(accountId));
        //前置已经检查币种存在
        currencyRepository.get(currencyId).ifPresent(
                currency -> doIncrease(wrapper, increase, account, currency));
    }

    public void on(NexusWrapper wrapper, Nexus.Decrease.Reader decrease) {
        int accountId = decrease.getAccountId();
        Result<Balance> result = accountRepository.get(accountId)
                .map(account -> account.decrease(decrease.getCurrencyId(), new BigDecimal(decrease.getAmount().toString())))
                .orElse(Result.fail(Nexus.RejectionReason.BALANCE_NOT_ENOUGH));
        if (result.isSuccess()) {
            publishDecreasedEvent(wrapper, result.value());
        } else {
            publishFailureEvent(wrapper, Nexus.EventType.DECREASE_REJECTED, result.reason());
        }
    }

    private void publishIncreasedEvent(NexusWrapper wrapper, Balance balance) {
        if (wrapper.isJournalEvent()) {
            return;
        }
        responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
            responseWrapper.setId(wrapper.getId());
            responseWrapper.setEventType(NexusWrapper.EventType.BUSINESS);
            transfer.writeBalance(balance, Nexus.EventType.INCREASED, responseWrapper.getBuffer());
        });
    }

    private void publishDecreasedEvent(NexusWrapper wrapper, Balance balance) {
        if (wrapper.isJournalEvent()) {
            return;
        }
        responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
            responseWrapper.setId(wrapper.getId());
            responseWrapper.setEventType(NexusWrapper.EventType.BUSINESS);
            transfer.writeBalance(balance, Nexus.EventType.DECREASED, responseWrapper.getBuffer());
        });
    }

    private void doIncrease(NexusWrapper wrapper, Nexus.Increase.Reader increase, Account account, Currency currency) {
        Result<Balance> result = account.increase(currency, new BigDecimal(increase.getAmount().toString()));//increase.amount.get());
        publishIncreasedEvent(wrapper, result.value());
    }

    private void publishFailureEvent(NexusWrapper wrapper, Nexus.EventType eventType, Nexus.RejectionReason rejectionReason) {
        if (wrapper.isJournalEvent()) {
            return;
        }
        responseRingBuffer.publishEvent((responseWrapper, sequence) -> {
            responseWrapper.setId(wrapper.getId());
            responseWrapper.setEventType(NexusWrapper.EventType.BUSINESS);
            transfer.writeFailed(eventType, rejectionReason, responseWrapper.getBuffer());
        });
    }


    public void on(Nexus.Unfreeze.Reader unfreeze) {
        int accountId = unfreeze.getAccountId();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.unfreeze(unfreeze.getCurrencyId(), new BigDecimal(unfreeze.getAmount().toString()));
    }

    public void on(Nexus.Clear.Reader clear) {
        int accountId = clear.getAccountId();
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
        account.settle(clear.getPayCurrencyId(), BigDecimalUtil.valueOf(clear.getPayAmount().toString()),
                BigDecimalUtil.valueOf(clear.getRefundAmount().toString()),
                currencyRepository.get(clear.getIncomeCurrencyId()).get(),
                BigDecimalUtil.valueOf(clear.getIncomeAmount().toString()));
    }

}
