package com.cmex.bolt.spot.service;

import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;
import com.lmax.disruptor.EventHandler;

public class SequencerDispatcher implements EventHandler<Message> {


    private final AccountService accountService;

    public SequencerDispatcher() {
        this.accountService = new AccountService();
    }

    public AccountService getAccountService() {
        return accountService;
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();
        switch (type) {
            case DEPOSIT:
                accountService.on(message.id.get(), message.payload.asDeposit);
                break;
            case WITHDRAW:
                accountService.on(message.id.get(), message.payload.asWithdraw);
                break;
            case UNFREEZE:
                accountService.on(message.id.get(), message.payload.asUnfreeze);
                break;
            case PLACE_ORDER:
                accountService.on(message.id.get(), message.payload.asPlaceOrder);
                break;
        }
    }
}