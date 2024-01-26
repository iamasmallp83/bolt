package com.cmex.bolt.spot.disruptor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.dsl.Disruptor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestDisruptor {
    @Test
    public void test() {
        // 创建一个 Disruptor 实例
        Disruptor<MyEvent> disruptor = new Disruptor<>(MyEvent::new, 1024, Executors.newFixedThreadPool(4));

// 创建消费者
        MyJournalConsumer journalConsumer = new MyJournalConsumer();
        MyReplicationConsumer replicationConsumer = new MyReplicationConsumer();
        MyApplicationConsumer applicationConsumer = new MyApplicationConsumer();

// 将消费者添加到 Disruptor，并将它们作为 gating consumers 添加
        disruptor.handleEventsWith(journalConsumer, replicationConsumer)
                .then(applicationConsumer);

// 将 journalConsumer 和 replicationConsumer 添加到 gating consumers 集合
        disruptor.getRingBuffer().addGatingSequences(journalConsumer.getSequence(), replicationConsumer.getSequence());

// 启动 Disruptor
        disruptor.start();
        disruptor.publishEvent(new EventTranslator<MyEvent>() {
            @Override
            public void translateTo(MyEvent event, long sequence) {

            }
        });
    }

    private class MyEvent {
    }

    private class MyJournalConsumer implements EventHandler<MyEvent> {
        public Sequence sequence = new Sequence(-1);

        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            System.out.println("journal consume");
            this.sequence.set(sequence);
        }

        public Sequence getSequence() {
            return sequence;
        }
    }

    private class MyReplicationConsumer implements EventHandler<MyEvent> {
        public Sequence sequence = new Sequence(-1);

        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            System.out.println("replication consume");
            this.sequence.set(sequence);
        }

        public Sequence getSequence() {
            return sequence;
        }
    }

    private class MyApplicationConsumer implements EventHandler<MyEvent> {
        @Override
        public void onEvent(MyEvent event, long sequence, boolean endOfBatch) throws Exception {
            System.out.println("application consume");
        }
    }
}
