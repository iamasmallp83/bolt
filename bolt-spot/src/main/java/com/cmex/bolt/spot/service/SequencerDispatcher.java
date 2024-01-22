package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.cmex.bolt.spot.util.OrderIdGenerator;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

import java.util.List;

public class SequencerDispatcher implements EventHandler<Message>, LifecycleAware {

    private final AccountService accountService;
    private List<MatchService> matchServices;

    public SequencerDispatcher(AccountService accountService) {
        this.accountService = accountService;
    }

    public AccountService getAccountService() {
        return accountService;
    }

    public void setMatchServices(List<MatchService> matchServices) {
        this.matchServices = matchServices;
    }

    public List<MatchService> getMatchServices() {
        return matchServices;
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();
        switch (type) {
            case INCREASE:
                accountService.on(message.id.get(), message.payload.asIncrease);
                break;
            case DECREASE:
                accountService.on(message.id.get(), message.payload.asDecrease);
                break;
            case CLEARED:
                accountService.on(message.id.get(), message.payload.asCleared);
                break;
            case UNFREEZE:
                accountService.on(message.id.get(), message.payload.asUnfreeze);
                break;
            case PLACE_ORDER:
                accountService.on(message.id.get(), message.payload.asPlaceOrder);
                break;
            case CANCEL_ORDER:
                int symbolId = OrderIdGenerator.getSymbolId(message.payload.asCancelOrder.orderId.get());
                matchServices.get(symbolId % 10).on(message.id.get(), message.payload.asCancelOrder);
                break;
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(SequencerDispatcher.class.getSimpleName() + "-thread");
    }

    @Override
    public void onShutdown() {

    }
}