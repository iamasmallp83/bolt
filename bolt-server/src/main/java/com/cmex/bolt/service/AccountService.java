package com.cmex.bolt.service;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Symbol;
import com.cmex.bolt.domain.*;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.util.Result;
import com.lmax.disruptor.RingBuffer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AccountService {

    private final AccountRepository accountRepository;
    private final CurrencyRepository currencyRepository;

    private final SymbolRepository symbolRepository;

    private RingBuffer<NexusWrapper> responseRingBuffer;
    private RingBuffer<NexusWrapper> matchRingBuffer;
    private final Transfer transfer = new Transfer();

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

    public void on(long messageId, Nexus.PlaceOrder placeOrder) {
        int symbolId = 0;
        symbolRepository.get(symbolId).ifPresentOrElse(
                symbol -> handleSymbolPresent(messageId, symbol, placeOrder),
                () -> handleSymbolAbsent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED)
        );
    }

    private void handleSymbolPresent(long messageId, Symbol symbol, Nexus.PlaceOrder placeOrder) {
        int accountId = 0;
        accountRepository.get(accountId).ifPresentOrElse(
                account -> handleAccountPresent(messageId, symbol, account, placeOrder),
                () -> handleAccountAbsent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED)
        );
    }

    private void handleSymbolAbsent(long messageId, Nexus.EventType eventType) {
        publishFailureEvent(messageId, eventType, Nexus.RejectionReason.SYMBOL_NOT_EXIST);
    }

    private void handleAccountPresent(long messageId, Symbol symbol, Account account, Nexus.PlaceOrder placeOrder) {
        Result<Balance> result = calculateAndFreezeAmount(symbol, account, placeOrder);
        if (result.isSuccess()) {
            publishPlaceOrderEvent(messageId, placeOrder);
        } else {
            publishFailureEvent(messageId, Nexus.EventType.PLACE_ORDER_REJECTED, result.reason());
        }
    }

    private void handleAccountAbsent(long messageId, Nexus.EventType eventType) {
        publishFailureEvent(messageId, eventType, Nexus.RejectionReason.BALANCE_NOT_ENOUGH);
    }

    private void publishPlaceOrderEvent(long messageId, Nexus.PlaceOrder placeOrder) {
        matchRingBuffer.publishEvent((message, sequence) -> {
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
        int accountId = 0;
        Result<Balance> result = accountRepository.get(accountId)
                .map(account -> account.decrease(0, 0))
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
            transfer.write(balance, Nexus.EventType.INCREASED, message.getBuffer());
        });
    }

    private void publishDecreasedEvent(long messageId, Balance balance) {
        responseRingBuffer.publishEvent((message, sequence) -> {
            message.setId(messageId);
        });
    }

    private Result<Balance> calculateAndFreezeAmount(Symbol symbol, Account account, Nexus.PlaceOrder placeOrder) {
//        if (placeOrder.side.get() == Nexus.OrderSide.BID) {
//            long volume;
//            if (placeOrder.volume.get() > 0) {
//                volume = placeOrder.volume.get();
//            } else {
//                volume = symbol.getVolume(placeOrder.price.get(), placeOrder.quantity.get());
//            }
//            if (symbol.isQuoteSettlement()) {
//                volume += Rate.getRate(volume, placeOrder.takerRate.get());
//            }
//            placeOrder.frozen.set(volume);
//            return account.freeze(symbol.getQuote().getId(), volume);
//        } else {
//            placeOrder.frozen.set(placeOrder.quantity.get());
//            return account.freeze(symbol.getBase().getId(), placeOrder.quantity.get());
//        }
        return null;
    }

    private void doIncrease(long messageId, Nexus.Increase.Reader increase, Account account, Currency currency) {
        Result<Balance> result = account.increase(currency, increase.getAmount());//increase.amount.get());
        publishIncreasedEvent(messageId, result.value());
    }

    private void publishFailureEvent(long messageId, Nexus.EventType eventType, Nexus.RejectionReason rejectionReason) {
        responseRingBuffer.publishEvent((message, sequence) -> {
//            rejectionReason.setMessage(message, messageId, eventType);
        });
    }


    public void on(long messageId, Nexus.Unfreeze unfreeze) {
        int accountId = 0;
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
//        account.unfreeze(unfreeze.currencyId.get(), unfreeze.amount.get());
        account.unfreeze(0, 0);
    }

    public void on(long messageId, Nexus.Cleared cleared) {
//        int accountId = cleared.accountId.get();
        int accountId = 0;
        Optional<Account> optional = accountRepository.get(accountId);
        Account account = optional.get();
//        account.settle(cleared.payCurrencyId.get(), cleared.payAmount.get(), cleared.refundAmount.get(),
//                currencyRepository.get(cleared.incomeCurrencyId.get()).get(), cleared.incomeAmount.get());
    }

    public void setMatchRingBuffer(RingBuffer<NexusWrapper> matchRingBuffer) {
        this.matchRingBuffer = matchRingBuffer;
    }

    public void setResponseRingBuffer(RingBuffer<NexusWrapper> responseRingBuffer) {
        this.responseRingBuffer = responseRingBuffer;
    }

}
