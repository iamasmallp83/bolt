package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 确认处理器 - 负责处理从节点的确认响应
 * 主节点专用：等待从节点确认，确保强一致性
 */
@Slf4j
public class ConfirmHandler implements EventHandler<NexusWrapper> {

    @Getter
    private final Sequence sequence = new Sequence();

    // 从节点确认管理
    private final Map<String, Sequence> slaveSequences = new ConcurrentHashMap<>();

    // 等待确认的序列号映射
    private final Map<Long, CountDownLatch> pendingConfirmations = new ConcurrentHashMap<>();

    public ConfirmHandler(BoltConfig config) {

        log.info("ConfirmHandler initialized - batchSize: {}, barrierTimeoutMs: {}",
                config.batchSize(), config.batchTimeout());
    }

    /**
     * 设置TcpReplicationServer引用
     */
    public void setTcpReplicationServer(TcpReplicationServer tcpReplicationServer) {
        // 通过setter方法设置
        log.info("TcpReplicationServer reference set in ConfirmHandler");
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理业务事件
        if (!wrapper.isBusinessEvent() || !wrapper.isValid()) {
            this.sequence.set(sequence);
            return;
        }
        
        // 重要：ConfirmHandler不阻塞处理链，立即设置sequence
        // 确认逻辑在handleSlaveConfirmation中异步处理
        this.sequence.set(sequence);
    }

    /**
     * 处理从节点确认
     */
    public void handleSlaveConfirmation(long sequence, String slaveNodeId) {
        log.debug("接收到确认 - sequence: {}, slave: {}", sequence, slaveNodeId);

        // 更新从节点的确认序列
        slaveSequences.computeIfAbsent(slaveNodeId, k -> new Sequence(-1)).set(sequence);

        // 获取最小从节点序列
        long minSlaveSequence = getMinSlaveSequence();

        // 如果所有从节点都已经确认到这个序列，记录确认状态
        if (minSlaveSequence >= sequence) {
            log.debug("All slaves confirmed sequence {}", sequence);

            // 通知等待的线程
            CountDownLatch latch = pendingConfirmations.get(sequence);
            if (latch != null) {
                latch.countDown();
                log.debug("Notified waiting thread for sequence {}", sequence);
            }
        }

        log.debug("Updated slave {} sequence to {}", slaveNodeId, sequence);
    }

    /**
     * 获取最小从节点序列
     */
    private long getMinSlaveSequence() {
        if (slaveSequences.isEmpty()) {
            return Long.MAX_VALUE;
        }

        long minSequence = Long.MAX_VALUE;
        for (Sequence slaveSequence : slaveSequences.values()) {
            long slaveSeq = slaveSequence.get();
            if (slaveSeq < minSequence) {
                minSequence = slaveSeq;
            }
        }

        return minSequence == Long.MAX_VALUE ? -1 : minSequence;
    }

    /**
     * 添加从节点
     */
    public void addSlave(String slaveNodeId) {
        slaveSequences.put(slaveNodeId, new Sequence(-1));
        log.info("Added slave to confirmation tracking - slave: {}", slaveNodeId);
    }

    /**
     * 移除从节点
     */
    public void removeSlave(String slaveNodeId) {
        slaveSequences.remove(slaveNodeId);
        log.info("Removed slave from confirmation tracking - slave: {}", slaveNodeId);
    }

}
