package com.cmex.bolt.server.handler;

import com.cmex.bolt.server.api.Message;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import lombok.Getter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class JournalHandler implements EventHandler<Message> {

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
    public void onEvent(Message message, long sequence, boolean endOfBatch) throws Exception {
        message.write(out);
        if (endOfBatch) {
            out.flush();
        }
        this.sequence.set(sequence);
    }

}
