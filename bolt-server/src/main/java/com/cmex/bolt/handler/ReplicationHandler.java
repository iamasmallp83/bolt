package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 主节点专用：发送复制请求
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private final BoltConfig config;
    private final ReplicationState replicationState;

    // TCP复制服务器引用（用于通过现有连接发送数据）
    private TcpReplicationServer tcpReplicationServer;

    // 并行处理线程池
    private final ExecutorService replicationExecutor;

    // 性能统计
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);

    public ReplicationHandler(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;

        // 创建专用线程池用于并行发送
        this.replicationExecutor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "replication-sender-" + r.hashCode());
                t.setDaemon(true);
                return t;
            }
        );

        log.info("ReplicationHandler initialized - replicationPort: {}, threads: {}",
                config.replicationPort(), Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查 - 只处理业务事件
        if (wrapper.shouldSkipProcessing()) {
            return;
        }

        // 检查buffer状态，如果被之前的处理器消费了，需要恢复
        int readableBytes = wrapper.getBuffer().readableBytes();
        log.debug("ReplicationHandler processing - sequence: {}, id: {}, readableBytes: {}, readerIndex: {}, writerIndex: {}",
                sequence, wrapper.getId(), readableBytes, wrapper.getBuffer().readerIndex(), wrapper.getBuffer().writerIndex());

        if (readableBytes == 0) {
            log.warn("NexusWrapper buffer has no readable bytes - sequence: {}, id: {}",
                    sequence, wrapper.getId());
            // 跳过复制，但继续处理链
            return;
        }

        totalEventsProcessed.incrementAndGet();
        
        // 发送复制请求到从节点
        sendReplicationRequest(wrapper, sequence);
        
    }

    /**
     * 发送复制请求到从节点
     */
    private void sendReplicationRequest(NexusWrapper wrapper, long sequence) {
        List<String> healthySlaves = replicationState.getHealthySlaveIds().stream().toList();
        if (healthySlaves.isEmpty()) {
            log.debug("没有健康的从节点，跳过复制 - sequence: {}", sequence);
            return;
        }

        log.debug("发送复制请求到从节点 - id: {}, sequence: {}, slaves: {}",
                wrapper.getId(), sequence, healthySlaves.size());

        // 并行发送到所有从节点
        List<CompletableFuture<Void>> sendTasks = healthySlaves.stream()
                .map(slaveNodeIdStr -> CompletableFuture.runAsync(() -> {
                    try {
                        // 将String转换为int nodeId
                        int nodeId = Integer.parseInt(slaveNodeIdStr);
                        
                        // 通过TcpReplicationServer发送复制请求
                        if (tcpReplicationServer != null) {
                            tcpReplicationServer.sendReplicationRequest(nodeId, wrapper, sequence);
                        }
                    } catch (Exception e) {
                        log.error("发送复制请求失败 - slave: {}, sequence: {}", slaveNodeIdStr, sequence, e);
                        replicationState.setSlaveConnected(Integer.parseInt(slaveNodeIdStr), false);
                    }
                }, replicationExecutor))
                .toList();

        // 等待所有发送完成
        CompletableFuture.allOf(sendTasks.toArray(new CompletableFuture[0]))
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("批量发送复制请求失败 - sequence: {}", sequence, throwable);
                    } else {
                        log.debug("复制请求发送完成 - sequence: {}", sequence);
                    }
                });
    }

    /**
     * 设置TcpReplicationServer引用
     */
    public void setTcpReplicationServer(TcpReplicationServer tcpReplicationServer) {
        this.tcpReplicationServer = tcpReplicationServer;
        log.info("TcpReplicationServer reference set in ReplicationHandler");
    }

    /**
     * 获取性能统计信息
     */
    public PerformanceStats getPerformanceStats() {
        return new PerformanceStats(totalEventsProcessed.get());
    }

    /**
     * 性能统计数据结构
     */
    public static class PerformanceStats {
        public final long totalEventsProcessed;
        
        public PerformanceStats(long totalEventsProcessed) {
            this.totalEventsProcessed = totalEventsProcessed;
        }
        
        @Override
        public String toString() {
            return String.format("PerformanceStats{events=%d}", totalEventsProcessed);
        }
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(ReplicationHandler.class.getSimpleName() + "-thread");
        log.info("ReplicationHandler started");
    }

    @Override
    public void onShutdown() {
        log.info("ReplicationHandler shutting down");
        
        // 关闭线程池
        if (replicationExecutor != null && !replicationExecutor.isShutdown()) {
            replicationExecutor.shutdown();
            log.info("ReplicationExecutor shutdown completed");
        }
        
        // 记录最终统计
        log.info("Final replication stats: {}", getPerformanceStats());
    }
}
