package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.core.NexusWrapper.EventType;
import com.cmex.bolt.Nexus;
import com.cmex.bolt.domain.Transfer;
import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.capnproto.MessageBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SnapshotTrigger {

    private final BoltConfig config;
    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    private final ScheduledExecutorService scheduler;

    public SnapshotTrigger(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.config = config;
        this.sequencerRingBuffer = sequencerRingBuffer;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("SnapshotTrigger-thread");
            t.setDaemon(true);
            return t;
        });

        // 启动定时任务
        startSnapshotSchedule();
        log.info("SnapshotTrigger started with interval: {}s", config.snapshotInterval());
    }

    private void startSnapshotSchedule() {
        long snapshotIntervalMs = config.snapshotInterval() * 1000L; // 转换为毫秒
        scheduler.scheduleWithFixedDelay(
            this::triggerSnapshot,
            snapshotIntervalMs, // 初始延迟
            snapshotIntervalMs, // 间隔
            TimeUnit.MILLISECONDS
        );
    }

    private void triggerSnapshot() {
        try {
            long timestamp = System.currentTimeMillis();
            
            sequencerRingBuffer.publishEvent((wrapper, sequence) -> {
                wrapper.setId(0); // snapshot事件没有业务ID
                wrapper.setPartition(-1); // snapshot事件没有分区，所有分区都需要处理
                wrapper.setEventType(EventType.INTERNAL);
                
                // 使用Transfer来序列化Snapshot事件
                Transfer transfer = new Transfer();
                transfer.writeSnapshotEvent(timestamp, wrapper.getBuffer());
            });
            
            log.debug("Snapshot triggered with timestamp: {}", timestamp);
        } catch (Exception e) {
            log.error("Failed to trigger snapshot", e);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("SnapshotTrigger shutdown completed");
    }
}
