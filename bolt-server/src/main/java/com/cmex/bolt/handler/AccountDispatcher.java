package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.repository.impl.AccountRepository;
import com.cmex.bolt.repository.impl.CurrencyRepository;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.cmex.bolt.service.AccountService;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import lombok.Setter;

@Getter
public class AccountDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {
    private final BoltConfig config;
    private final int group;
    private final int partition;
    private final AccountService accountService;
    private final Transfer transfer;
    private final SequencerSnapshotHandler sequencerSnapshot;

    @Setter
    private RingBuffer<NexusWrapper> matchingRingBuffer;

    public AccountDispatcher(BoltConfig config, int group, int partition,
                             AccountRepository accountRepository,
                             CurrencyRepository currencyRepository,
                             SymbolRepository symbolRepository) {
        this.config = config;
        this.group = group;
        this.partition = partition;
        this.accountService = new AccountService(group, accountRepository, currencyRepository, symbolRepository);
        this.transfer = new Transfer();
        this.sequencerSnapshot = new SequencerSnapshotHandler(config, accountService);
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
        // Snapshot事件没有分区，所有分区都需要处理
        if (partition != wrapper.getPartition() && !wrapper.isSnapshotEvent()) {
            return;
        }
        Nexus.NexusEvent.Reader reader = transfer.from(wrapper.getBuffer().copy());
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
            case SNAPSHOT:
                // 处理Sequencer快照
                sequencerSnapshot.handleSnapshot(reader, partition);
                // 转发Snapshot到matching ring buffer
                publishSnapshotEvent(wrapper, reader);
                break;
            case CANCEL_ORDER:
                publishCancelOrderEvent(wrapper, reader);
                break;
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
                accountService.on(wrapper, clear);
                break;
            case UNFREEZE:
                Nexus.Unfreeze.Reader unfreeze = reader.getPayload().getUnfreeze();
                accountService.on(wrapper, unfreeze);
                break;
            case PLACE_ORDER:
                Nexus.PlaceOrder.Reader placeOrder = reader.getPayload().getPlaceOrder();
                accountService.on(wrapper, placeOrder);
                break;
            default:
                break;
        }
    }

    private void publishCancelOrderEvent(NexusWrapper wrapper, Nexus.NexusEvent.Reader reader) {
        matchingRingBuffer.publishEvent((matchingWrapper, sequence) -> {
            matchingWrapper.setId(wrapper.getId());
            matchingWrapper.setPartition(partition); // 发送到所有matching partition
            matchingWrapper.setEventType(wrapper.getEventType());
            matchingWrapper.getBuffer().writeBytes(wrapper.getBuffer());
            wrapper.getBuffer().resetReaderIndex();
        });
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(AccountDispatcher.class.getSimpleName() + "-" + partition + "-thread");
    }

    /**
     * 将Snapshot事件转发到matching ring buffer
     */
    private void publishSnapshotEvent(NexusWrapper wrapper, Nexus.NexusEvent.Reader reader) {
        matchingRingBuffer.publishEvent((matchingWrapper, sequence) -> {
            matchingWrapper.setId(wrapper.getId());
            matchingWrapper.setPartition(partition); // 发送到所有matching partition
            transfer.writeSnapshotEvent(reader, matchingWrapper.getBuffer());
        });
    }

    @Override
    public void onShutdown() {

    }
}