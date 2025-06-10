package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import lombok.Getter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class JournalHandler implements EventHandler<NexusWrapper> {

    @Getter
    private final Sequence sequence = new Sequence();

    private final FileOutputStream out;

    public JournalHandler() {
        try {
            this.out = new FileOutputStream("journal.data");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(NexusWrapper message, long sequence, boolean endOfBatch) throws Exception {
    }

}
