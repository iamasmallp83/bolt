package com.cmex.bolt.replication;

import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 复制上下文 - 用于解耦复制组件之间的依赖
 * 避免循环依赖问题
 */
@Slf4j
@Getter
public class ReplicationContext {

    private volatile RingBuffer<NexusWrapper> sequencerRingBuffer;
    private volatile boolean initialized = false;

    public ReplicationContext() {
    }

    /**
     * 设置SequencerRingBuffer（延迟注入）
     */
    public void setSequencerRingBuffer(RingBuffer<NexusWrapper> sequencerRingBuffer) {
        // 第一次检查（无锁）
        if (this.sequencerRingBuffer != null) {
            log.warn("SequencerRingBuffer already set, ignoring duplicate call");
            return;
        }

        // 获取锁
        synchronized (this) {
            // 第二次检查（有锁）- 防止多线程竞争
            if (this.sequencerRingBuffer == null) {
                this.sequencerRingBuffer = sequencerRingBuffer;
                this.initialized = true;
                log.info("SequencerRingBuffer injected into ReplicationContext");
                notifyAll();  // 唤醒所有等待的线程
            }
        }
    }

    /**
     * 获取SequencerRingBuffer（带检查）
     */
    public RingBuffer<NexusWrapper> getSequencerRingBuffer() throws InterruptedException {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    wait();
                }
            }
        }
        return sequencerRingBuffer;
    }
}
