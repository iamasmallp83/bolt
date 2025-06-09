package com.cmex.bolt.server.handler;

import com.cmex.bolt.server.api.*;
import com.cmex.bolt.server.service.AccountService;
import com.cmex.bolt.server.service.MatchService;
import com.cmex.bolt.server.util.OrderIdGenerator;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
public class AccountDispatcher implements EventHandler<Message>, LifecycleAware {
    private final int amount;

    private final int partition;

    private final AccountService accountService;
    @Setter
    private List<MatchService> matchServices;

    public AccountDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.accountService = new AccountService();
    }

    public void onEvent(Message message, long sequence, boolean endOfBatch) {
        EventType type = message.type.get();
        switch (type) {
            case INCREASE:
                Increase increase = message.payload.asIncrease;
                if (partition == increase.accountId.get() % 10) {
                    accountService.on(message.id.get(), increase);
                }
                break;
            case DECREASE:
                Decrease decrease = message.payload.asDecrease;
                if (partition == decrease.accountId.get() % 10) {
                    accountService.on(message.id.get(), decrease);
                }
                break;
            case CLEARED:
                Cleared cleared = message.payload.asCleared;
                if (partition == cleared.accountId.get() % 10) {
                    accountService.on(message.id.get(), cleared);
                }
                break;
            case UNFREEZE:
                Unfreeze unfreeze = message.payload.asUnfreeze;
                if (partition == unfreeze.accountId.get() % 10) {
                    accountService.on(message.id.get(), unfreeze);
                }
                break;
            case PLACE_ORDER:
                PlaceOrder placeOrder = message.payload.asPlaceOrder;
                if (partition == placeOrder.accountId.get() % 10) {
                    accountService.on(message.id.get(), placeOrder);
                }
                break;
            case CANCEL_ORDER:
                int symbolId = OrderIdGenerator.getSymbolId(message.payload.asCancelOrder.orderId.get());
                matchServices.get(symbolId % 10).on(message.id.get(), message.payload.asCancelOrder);
                break;
            default:
                break;
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(AccountDispatcher.class.getSimpleName() + "-" + partition + "-thread");
    }

    @Override
    public void onShutdown() {

    }
}