package com.cmex.bolt.server.handler;

import com.cmex.bolt.server.api.Message;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import lombok.Getter;

@Getter
public class DataSourceHandler implements EventHandler<Message> {

    private final Sequence sequence;

    public DataSourceHandler() {
        sequence = new Sequence();
    }

    @Override
    public void onEvent(Message message, long sequence, boolean endOfBatch) throws Exception {}

}
