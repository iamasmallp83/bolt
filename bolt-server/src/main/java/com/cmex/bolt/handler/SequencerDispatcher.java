package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.service.AccountService;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import lombok.Setter;

@Getter
public class SequencerDispatcher implements EventHandler<NexusWrapper>, LifecycleAware {
    private final BoltConfig config;
    private final int group;
    private final int partition;
    private final AccountService accountService;
    private final Transfer transfer;
    private final SnapshotHandler snapshotHandler;
    
    @Setter
    private RingBuffer<NexusWrapper> matchingRingBuffer;

    public SequencerDispatcher(BoltConfig config, int group, int partition) {
        this.config = config;
        this.group = group;
        this.partition = partition;
        this.accountService = new AccountService(group);
        this.transfer = new Transfer();
        this.snapshotHandler = new SnapshotHandler(config);
    }

    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) {
        // Snapshot事件没有分区，所有分区都需要处理
        if (partition != wrapper.getPartition() && wrapper.getPartition() != -1) {
            return;
        }
        
        // 检查buffer状态，如果被之前的处理器消费了，需要恢复
        int readableBytes = wrapper.getBuffer().readableBytes();
        if (readableBytes == 0) {
            // Buffer已被消费，跳过处理
            return;
        }
        
        // 确保buffer的readerIndex在正确位置
        wrapper.getBuffer().readerIndex(0);
        
        Nexus.NexusEvent.Reader reader = transfer.from(wrapper.getBuffer());
        Nexus.Payload.Reader payload = reader.getPayload();
        switch (payload.which()) {
            case SNAPSHOT:
                // Snapshot事件只需要first partition处理（partition 0）
                if (partition == 0) {
                    snapshotHandler.handleSnapshot(reader);
                }
                // 转发Snapshot到matching ring buffer
                publishSnapshotEvent(wrapper, reader);
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

    /**
     * 将Snapshot事件转发到matching ring buffer
     */
    private void publishSnapshotEvent(NexusWrapper wrapper, Nexus.NexusEvent.Reader reader) {
        if (matchingRingBuffer != null) {
            // 转发到所有matching dispatchers
            for (int i = 0; i < group; i++) {
                int finalPartition = i;
                matchingRingBuffer.publishEvent((matchingWrapper, sequence) -> {
                    matchingWrapper.setId(wrapper.getId());
                    matchingWrapper.setPartition(finalPartition); // 发送到所有matching partition
                    transfer.writeSnapshotEvent(reader, matchingWrapper.getBuffer());
                });
            }
        }
    }

    @Override
    public void onShutdown() {

    }
}