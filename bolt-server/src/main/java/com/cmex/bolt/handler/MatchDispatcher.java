package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.service.MatchService;
import com.cmex.bolt.util.OrderIdGenerator;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.Getter;

public class MatchDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {

    private final int group;
    @Getter
    private final int partition;
    @Getter
    private final MatchService matchService;

    private final Transfer transfer;

    public MatchDispatcher(int group, int partition) {
        this.group = group;
        this.partition = partition;
        this.matchService = new MatchService(group);
        this.transfer = new Transfer();
    }

    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
        if (partition != wrapper.getPartition()) {
            return;
        }
        Nexus.NexusEvent.Reader reader = transfer.from(wrapper.getBuffer());
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
            case CANCEL_ORDER:
                Nexus.CancelOrder.Reader cancelOrder = reader.getPayload().getCancelOrder();
                if (partition == OrderIdGenerator.getSymbolId(cancelOrder.getOrderId()) % group) {
                    matchService.on(wrapper.getId(), cancelOrder);
                }
                break;
            case PLACE_ORDER:
                Nexus.PlaceOrder.Reader placeOrder = reader.getPayload().getPlaceOrder();
                matchService.on(wrapper.getId(), placeOrder);
                break;
            default:
                break;
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(MatchDispatcher.class.getSimpleName() + "-" + partition + "-thread");
    }

    @Override
    public void onShutdown() {

    }
}