package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.service.AccountService;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;

@Getter
public class SequencerDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {
    private final int group;

    private final int partition;

    private final AccountService accountService;

    private final Transfer transfer;

    public SequencerDispatcher(int group, int partition) {
        this.group = group;
        this.partition = partition;
        this.accountService = new AccountService(group);
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
                accountService.on(wrapper, increase);
                break;
            case DECREASE:
                Nexus.Decrease.Reader decrease = reader.getPayload().getDecrease();
                accountService.on(wrapper, decrease);
                break;
            case CLEAR:
                Nexus.Clear.Reader clear = reader.getPayload().getClear();
                accountService.on(clear);
                break;
            case UNFREEZE:
                Nexus.Unfreeze.Reader unfreeze = reader.getPayload().getUnfreeze();
                accountService.on(unfreeze);
                break;
            case PLACE_ORDER:
                Nexus.PlaceOrder.Reader placeOrder = reader.getPayload().getPlaceOrder();
                accountService.on(wrapper, placeOrder);
                break;
            default:
                break;
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(SequencerDispatcher.class.getSimpleName() + "-" + partition + "-thread");
    }

    @Override
    public void onShutdown() {

    }
}