package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 确认处理器 - 从节点专用
 * 负责确认事件已写入并发送确认响应给主节点
 */
@Slf4j
public class AckHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    @Getter
    private final Sequence sequence = new Sequence();

    private final BoltConfig config;
    private TcpReplicationClient tcpReplicationClient; // 改为非final，稍后设置
    
    // 性能统计
    private final AtomicLong totalEventsAcknowledged = new AtomicLong(0);
    
    public AckHandler(BoltConfig config) {
        this.config = config;
        this.tcpReplicationClient = null; // 稍后设置
        
        log.info("AckHandler initialized for slave node");
    }
    
    /**
     * 设置TcpReplicationClient引用
     */
    public void setTcpReplicationClient(TcpReplicationClient tcpReplicationClient) {
        this.tcpReplicationClient = tcpReplicationClient;
        log.info("TcpReplicationClient reference set in AckHandler");
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理业务事件
//        if (!wrapper.isBusinessEvent() || !wrapper.isValid()) {
//            this.sequence.set(sequence);
//            return;
//        }
        
        totalEventsAcknowledged.incrementAndGet();
        // 发送确认响应给主节点
        sendAcknowledgment(sequence);
        
        // 重要：设置sequence以继续处理链
        this.sequence.set(sequence);
    }
    
    /**
     * 发送确认响应给主节点
     */
    private void sendAcknowledgment(long sequence) {
        if (tcpReplicationClient != null && tcpReplicationClient.isConnected()) {
            try {
                tcpReplicationClient.sendConfirmation(sequence);
                log.debug("Sent acknowledgment to master - sequence: {}", sequence);
            } catch (Exception e) {
                log.error("Failed to send acknowledgment to master - sequence: {}", sequence, e);
            }
        } else {
            log.warn("TcpReplicationClient not connected, cannot send acknowledgment - sequence: {}", sequence);
        }
    }
    
    /**
     * 获取性能统计信息
     */
    public PerformanceStats getPerformanceStats() {
        return new PerformanceStats(totalEventsAcknowledged.get());
    }
    
    /**
     * 性能统计数据结构
     */
    public static class PerformanceStats {
        public final long totalEventsAcknowledged;
        
        public PerformanceStats(long totalEventsAcknowledged) {
            this.totalEventsAcknowledged = totalEventsAcknowledged;
        }
        
        @Override
        public String toString() {
            return String.format("PerformanceStats{acknowledged=%d}", totalEventsAcknowledged);
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(AckHandler.class.getSimpleName() + "-thread");
        log.info("AckHandler started");
    }

    @Override
    public void onShutdown() {
        log.info("AckHandler shutting down");
        
        // 记录最终统计
        log.info("Final acknowledgment stats: {}", getPerformanceStats());
    }
}
