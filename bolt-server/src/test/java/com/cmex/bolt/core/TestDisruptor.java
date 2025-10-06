package com.cmex.bolt.core;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class TestDisruptor {

    private class TestEvent {
        private long value;

        public void set(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "LongEvent{" + "value=" + value + '}';
        }
    }

    private class TestHandler implements EventHandler<TestEvent> {

        private String name;

        private int sleepTime;

        public TestHandler(String name, int sleepTime) {
            this.name = name;
            this.sleepTime = sleepTime;
        }

        @Override
        public void onEvent(TestEvent event, long sequence, boolean endOfBatch) {
            System.out.println(name + " Event: " + event);
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    @Test
    public void testDisruptor() throws InterruptedException {
        int bufferSize = 1024;

        Disruptor<TestEvent> disruptor =
                new Disruptor<>(TestEvent::new, bufferSize, DaemonThreadFactory.INSTANCE);

        disruptor.handleEventsWith(new TestHandler("A", 100), new TestHandler("B", 1000));
        disruptor.start();


        RingBuffer<TestEvent> ringBuffer = disruptor.getRingBuffer();
        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; true; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            Thread.sleep(100);
        }
    }
}
