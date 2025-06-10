package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import lombok.Getter;

@Getter
public class ReplicationHandler implements EventHandler<NexusWrapper> {


    private final Sequence sequence;

    public ReplicationHandler() {
        sequence = new Sequence();
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        this.sequence.set(sequence);
    }

}
