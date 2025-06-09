package com.cmex.bolt.server.disruptor;

import com.cmex.bolt.server.api.Message;
import com.google.common.base.Stopwatch;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DecodeMessage {

    @Test
    public void decode() throws IOException {
        try (FileInputStream input = new FileInputStream("journal.data")) {
            Message message = new Message();
            byte[] bytes = new byte[message.size()];
            long counter = 0;
            Stopwatch stopwatch = Stopwatch.createStarted();
            while (input.read(bytes) != -1) {
                message.getByteBuffer().put(bytes);
                switch (message.type.get()) {
                    case INCREASE:
                        break;
                    case PLACE_ORDER:
                        break;
                    default:
                        System.out.println(message);
                }
                message.getByteBuffer().clear();
                counter++;
            }
            System.out.println("read " + counter + " done");
            System.out.println("elapsed " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }
}
