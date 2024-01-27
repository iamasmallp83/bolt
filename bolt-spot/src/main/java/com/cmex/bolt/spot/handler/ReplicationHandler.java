package com.cmex.bolt.spot.handler;

import com.cmex.bolt.spot.api.Message;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.Sequence;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import lombok.Getter;
import org.agrona.BufferUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Getter
public class ReplicationHandler implements EventHandler<Message> {

    private static final String CHANNEL = "bolt";

    private static final int STREAM_ID = 0;

    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

    private final Sequence sequence;

    private final MediaDriver driver;

    private final Aeron aeron;

    private final Publication publication;

    public ReplicationHandler() {
        sequence = new Sequence();
        driver = MediaDriver.launchEmbedded();
        aeron = Aeron.connect(new Aeron.Context());
        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        Executors.newSingleThreadExecutor().execute(() -> {
            final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.println(String.format(
                        "Message to stream %d from session %d (%d@%d) <<%s>>",
                        STREAM_ID, header.sessionId(), length, offset, new String(data)));
            };

            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                    100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            while (true) {
                final int fragmentsRead = subscription.poll(fragmentHandler, 10);
                idleStrategy.idle(fragmentsRead);
            }
        });
    }

    @Override
    public void onEvent(Message message, long sequence, boolean endOfBatch) throws Exception {
        byte[] bytes = message.getByteBuffer().array();
        BUFFER.putBytes(0, bytes);
        publication.offer(BUFFER, 0, bytes.length);
        this.sequence.set(sequence);
    }

}
