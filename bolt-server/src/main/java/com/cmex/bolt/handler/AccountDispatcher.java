package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.service.AccountService;
import com.cmex.bolt.service.MatchService;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
public class AccountDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {
    private final int amount;

    private final int partition;

    private final AccountService accountService;
    @Setter
    private List<MatchService> matchServices;

    private final Transfer transfer;

    public AccountDispatcher(int amount, int partition) {
        this.amount = amount;
        this.partition = partition;
        this.accountService = new AccountService();
        this.transfer = new Transfer();
    }

    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
        if (partition != wrapper.getPartition()) {
            return;
        }
        Nexus.NexusEvent.Reader reader = transfer.from(wrapper.getBuffer());
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
            case INCREASE:
                Nexus.Increase.Reader increase = reader.getPayload().getIncrease();
                accountService.on(wrapper.getId(), increase);
                break;
//            case DECREASE:
//                Decrease decrease = message.payload.asDecrease;
//                if (partition == decrease.accountId.get() % 10) {
//                    accountService.on(message.id.get(), decrease);
//                }
//                break;
//            case CLEARED:
//                Cleared cleared = message.payload.asCleared;
//                if (partition == cleared.accountId.get() % 10) {
//                    accountService.on(message.id.get(), cleared);
//                }
//                break;
//            case UNFREEZE:
//                Unfreeze unfreeze = message.payload.asUnfreeze;
//                if (partition == unfreeze.accountId.get() % 10) {
//                    accountService.on(message.id.get(), unfreeze);
//                }
//                break;
//            case PLACE_ORDER:
//                PlaceOrder placeOrder = message.payload.asPlaceOrder;
//                if (partition == placeOrder.accountId.get() % 10) {
//                    accountService.on(message.id.get(), placeOrder);
//                }
//                break;
//            case CANCEL_ORDER:
//                int symbolId = OrderIdGenerator.getSymbolId(message.payload.asCancelOrder.orderId.get());
//                matchServices.get(symbolId % 10).on(message.id.get(), message.payload.asCancelOrder);
//                break;
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