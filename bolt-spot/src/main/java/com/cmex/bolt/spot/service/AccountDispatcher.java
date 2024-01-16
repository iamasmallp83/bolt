package com.cmex.bolt.spot.service;

import com.lmax.disruptor.EventHandler;
import com.cmex.bolt.spot.api.EventType;
import com.cmex.bolt.spot.api.Message;

public class AccountDispatcher implements EventHandler<Message> {

    private int amount;
    private int partition;

    private final AccountService accountService;

    public AccountDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.accountService = new AccountService();
    }

    public int getPartition() {
        return partition;
    }

    public AccountService getAccountService() {
        return accountService;
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();
        switch (type) {
            case DEPOSIT:
                if (partition == message.payload.asDeposit.accountId.get() % amount) {
                    accountService.on(message.id.get(), message.payload.asDeposit);
                }
                break;
            case WITHDRAW:
                if (partition == message.payload.asWithdraw.accountId.get() % amount) {
                    accountService.on(message.id.get(), message.payload.asWithdraw);
                }
                break;
            case UNFREEZE:
                if (partition == message.payload.asUnfreeze.accountId.get() % amount) {
                    accountService.on(message.id.get(), message.payload.asUnfreeze);
                }
                break;
            case PLACE_ORDER:
                if (partition == message.payload.asPlaceOrder.accountId.get() % amount) {
                    accountService.on(message.id.get(), message.payload.asPlaceOrder);
                }
                break;
        }
    }
}