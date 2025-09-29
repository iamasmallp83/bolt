package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 整合了高性能特性：并行发送、异步处理、性能监控
 * 配合BarrierHandler实现强一致性保证
 */
@Slf4j
public class ReplicationHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    @Getter
    private final Sequence sequence = new Sequence();
    
    private final BoltConfig config;
    private final ReplicationState replicationState;
    private final List<NexusWrapper> currentBatch;
    private final AtomicLong batchSequenceStart = new AtomicLong(-1);
    private final AtomicLong batchSequenceEnd = new AtomicLong(-1);
    
    // TCP复制服务器引用（用于通过现有连接发送数据）
    private TcpReplicationServer tcpReplicationServer;
    
    // 客户端连接管理（保留用于兼容性）
    private final Map<String, TcpReplicationClient> clients = new ConcurrentHashMap<>();
    
    // 并行处理线程池（从OptimizedReplicationHandler整合）
    private final ExecutorService replicationExecutor;
    
    // 性能统计（从OptimizedReplicationHandler整合）
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalSendTime = new AtomicLong(0);
    
    // 批次ID生成器和当前批次跟踪
    private final AtomicLong batchIdGenerator = new AtomicLong(1);
    private volatile long currentBatchId = -1;
    
    public ReplicationHandler(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;
        this.currentBatch = new ArrayList<>(config.batchSize());
        
        // 创建专用线程池用于并行发送
        this.replicationExecutor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "replication-sender-" + r.hashCode());
                t.setDaemon(true);
                return t;
            }
        );
        
        log.info("ReplicationHandler initialized - batchSize: {}, replicationPort: {}, threads: {}", 
                config.batchSize(), config.replicationPort(), Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查
        if (wrapper.getBuffer() == null || wrapper.getBuffer().readableBytes() == 0 || wrapper.getId() == -1) {
            this.sequence.set(sequence);
            return;
        }
        
        totalEventsProcessed.incrementAndGet();
        
        // 添加到当前批次
        synchronized (currentBatch) {
            currentBatch.add(wrapper);
            
            // 设置批次序列范围
            if (batchSequenceStart.get() == -1) {
                batchSequenceStart.set(sequence);
            }
            batchSequenceEnd.set(sequence);
            
            // 检查是否需要发送批次
            boolean shouldSendBatch = currentBatch.size() >= config.batchSize() || endOfBatch;
            
            if (shouldSendBatch) {
                // 同步发送批次，确保消息顺序和状态一致性
                sendBatchToSlaves();
            }
        }
        
        this.sequence.set(sequence);
    }
    
    /**
     * 异步发送批次到从节点
     */
    private void sendBatchToSlavesAsync() {
        List<NexusWrapper> batchToSend;
        long sequenceStart, sequenceEnd;
        
        synchronized (currentBatch) {
            if (currentBatch.isEmpty()) {
                log.debug("No events in current batch, skipping send");
                return;
            }
            
            batchToSend = new ArrayList<>(currentBatch);
            sequenceStart = batchSequenceStart.get();
            sequenceEnd = batchSequenceEnd.get();
            
            // 清空当前批次
            currentBatch.clear();
            batchSequenceStart.set(-1);
            batchSequenceEnd.set(-1);
        }
        
        // 创建批次跟踪器
        ReplicationState.BatchAckTracker tracker = replicationState.createBatchTracker(
                config.batchTimeout(), sequenceStart, sequenceEnd);
        
        log.info("Sending batch {} with {} events, sequences {}-{}", 
                tracker.getBatchId(), batchToSend.size(), sequenceStart, sequenceEnd);
        
        // 并行发送到所有从节点
        sendBatchToSlavesParallel(tracker.getBatchId(), batchToSend, sequenceStart, sequenceEnd);
        
        totalBatchesSent.incrementAndGet();
    }
    
    /**
     * 发送批次到从节点（同步版本，确保消息顺序）
     */
    private void sendBatchToSlaves() {
        List<NexusWrapper> batchToSend;
        long sequenceStart, sequenceEnd;
        long batchId;
        
        synchronized (currentBatch) {
            if (currentBatch.isEmpty()) {
                log.debug("No events in current batch, skipping send");
                return;
            }
            
            batchToSend = new ArrayList<>(currentBatch);
            sequenceStart = batchSequenceStart.get();
            sequenceEnd = batchSequenceEnd.get();
            
            // 生成批次ID并设置当前批次ID
            batchId = batchIdGenerator.getAndIncrement();
            currentBatchId = batchId;
            
            // 清空当前批次
            currentBatch.clear();
            batchSequenceStart.set(-1);
            batchSequenceEnd.set(-1);
        }
        
        // 创建批次跟踪器
        ReplicationState.BatchAckTracker tracker = replicationState.createBatchTracker(
                config.batchTimeout(), sequenceStart, sequenceEnd);
        
        log.info("Sending batch {} with {} events, sequences {}-{}", 
                batchId, batchToSend.size(), sequenceStart, sequenceEnd);
        
        // 发送到所有从节点（同步发送，确保顺序）
        sendBatchToSlaves(batchId, batchToSend, sequenceStart, sequenceEnd);
        
        // 等待确认（非阻塞方式）
        waitForBatchAcknowledgment(tracker);
        
        // 重置当前批次ID
        currentBatchId = -1;
    }
    
    /**
     * 等待批次确认
     */
    private void waitForBatchAcknowledgment(ReplicationState.BatchAckTracker tracker) {
        // 使用异步方式等待确认，避免阻塞主流程
        new Thread(() -> {
            long startTime = System.currentTimeMillis();
            
            while (!tracker.isAllAcknowledged() && !tracker.isTimeout()) {
                try {
                    Thread.sleep(10); // 短暂休眠，避免CPU占用过高
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
            long waitTime = System.currentTimeMillis() - startTime;
            
            if (tracker.isAllAcknowledged()) {
                log.debug("Batch {} acknowledged by all slaves in {}ms", 
                        tracker.getBatchId(), waitTime);
            } else if (tracker.isTimeout()) {
                log.warn("Batch {} timeout after {}ms, {} slaves pending: {}", 
                        tracker.getBatchId(), waitTime, tracker.getPendingCount(), 
                        tracker.getTimeoutSlaves());
                
                // 处理超时的从节点
                handleSlaveTimeout(tracker);
            }
        }, "batch-ack-waiter-" + tracker.getBatchId()).start();
    }
    
    /**
     * 处理从节点超时
     */
    private void handleSlaveTimeout(ReplicationState.BatchAckTracker tracker) {
        for (String slaveNodeId : tracker.getTimeoutSlaves()) {
            log.warn("Slave node {} timeout for batch {}", slaveNodeId, tracker.getBatchId());
            
            // 标记从节点为不健康状态
            replicationState.setSlaveConnected(slaveNodeId, false);
            
            // 可以在这里实现重连逻辑或故障转移
        }
    }
    
    /**
     * 处理从节点的确认
     */
    public void handleBatchAcknowledgment(long batchId, String slaveNodeId) {
        boolean acknowledged = replicationState.acknowledgeBatch(batchId, slaveNodeId);
        if (acknowledged) {
            log.debug("Received ack for batch {} from slave {}", batchId, slaveNodeId);
        } else {
            log.warn("Received invalid ack for batch {} from slave {}", batchId, slaveNodeId);
        }
    }
    
    /**
     * 发送批次到所有从节点（通过现有连接）
     */
    private void sendBatchToSlaves(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        Map<String, ReplicationState.SlaveNode> slaves = replicationState.getAllSlaves();
        
        log.info("Attempting to send batch {} to {} slaves", batchId, slaves.size());
        
        if (slaves.isEmpty()) {
            log.warn("No slaves available for batch {}", batchId);
            return;
        }
        
        if (tcpReplicationServer == null) {
            log.error("TcpReplicationServer reference not set, cannot send batch {}", batchId);
            return;
        }
        
        for (Map.Entry<String, ReplicationState.SlaveNode> entry : slaves.entrySet()) {
            String slaveNodeId = entry.getKey();
            ReplicationState.SlaveNode slave = entry.getValue();
            
            log.debug("Processing slave {}: host={}, port={}, healthy={}", 
                    slaveNodeId, slave.getHost(), slave.getPort(), slave.isHealthy(30000));
            
            if (slave.isHealthy(30000)) { // 30秒超时
                try {
                    log.debug("Sending batch {} to slave {} via existing connection", 
                            batchId, slaveNodeId);
                    tcpReplicationServer.sendBatchToSlave(slaveNodeId, batchId, events, sequenceStart, sequenceEnd);
                    log.debug("Successfully sent batch {} to slave {}", batchId, slaveNodeId);
                } catch (Exception e) {
                    log.error("Failed to send batch {} to slave {}: {}", 
                            batchId, slaveNodeId, e.getMessage(), e);
                    replicationState.setSlaveConnected(slaveNodeId, false);
                }
            } else {
                log.debug("Skipping unhealthy slave {} for batch {}", slaveNodeId, batchId);
            }
        }
    }
    
    /**
     * 并行发送批次到所有从节点（高性能版本）
     */
    private void sendBatchToSlavesParallel(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        Map<String, ReplicationState.SlaveNode> slaves = replicationState.getAllSlaves();
        
        if (slaves.isEmpty()) {
            log.warn("No slaves available for batch {}", batchId);
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        // 创建并行发送任务
        List<CompletableFuture<Void>> sendTasks = new ArrayList<>();
        
        for (Map.Entry<String, ReplicationState.SlaveNode> entry : slaves.entrySet()) {
            String slaveNodeId = entry.getKey();
            ReplicationState.SlaveNode slave = entry.getValue();
            
            if (slave.isHealthy(30000)) {
                CompletableFuture<Void> sendTask = CompletableFuture.runAsync(() -> {
                    try {
                        TcpReplicationClient client = getOrCreateClient(slaveNodeId, slave);
                        if (client != null) {
                            log.debug("Sending batch {} to slave {} ({}:{})", 
                                    batchId, slaveNodeId, slave.getHost(), slave.getPort());
                            client.sendBatch(batchId, events, sequenceStart, sequenceEnd);
                            log.debug("Successfully sent batch {} to slave {}", batchId, slaveNodeId);
                        } else {
                            log.warn("Failed to create client for slave {} ({}:{})", 
                                    slaveNodeId, slave.getHost(), slave.getPort());
                        }
                    } catch (Exception e) {
                        log.error("Failed to send batch {} to slave {} ({}:{}): {}", 
                                batchId, slaveNodeId, slave.getHost(), slave.getPort(), e.getMessage(), e);
                        replicationState.setSlaveConnected(slaveNodeId, false);
                    }
                }, replicationExecutor);
                
                sendTasks.add(sendTask);
            } else {
                log.debug("Skipping unhealthy slave {} for batch {}", slaveNodeId, batchId);
            }
        }
        
        // 等待所有发送任务完成
        CompletableFuture.allOf(sendTasks.toArray(new CompletableFuture[0]))
            .whenComplete((result, throwable) -> {
                long sendTime = System.currentTimeMillis() - startTime;
                totalSendTime.addAndGet(sendTime);
                
                if (throwable != null) {
                    log.error("Error in parallel batch sending for batch {}: {}", batchId, throwable.getMessage());
                } else {
                    log.debug("Parallel batch {} sent to {} slaves in {}ms", 
                            batchId, sendTasks.size(), sendTime);
                }
            });
    }
    
    /**
     * 获取或创建复制客户端
     */
    private TcpReplicationClient getOrCreateClient(String slaveNodeId, ReplicationState.SlaveNode slave) {
        log.debug("Getting or creating TCP client for slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
        
        TcpReplicationClient existingClient = clients.get(slaveNodeId);
        if (existingClient != null) {
            log.debug("Using existing TCP client for slave {}", slaveNodeId);
            return existingClient;
        }
        
        log.info("Creating new TCP replication client for slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
        
        return clients.computeIfAbsent(slaveNodeId, id -> {
            try {
                TcpReplicationClient client = new TcpReplicationClient(slave.getHost(), slave.getPort(), slaveNodeId);
                log.debug("Connecting to slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
                client.connect();
                replicationState.setSlaveConnected(slaveNodeId, true);
                log.info("Successfully created and connected TCP replication client for slave {} at {}:{}", 
                        slaveNodeId, slave.getHost(), slave.getPort());
                return client;
            } catch (Exception e) {
                log.error("Failed to create TCP replication client for slave {} at {}:{}: {}", 
                        slaveNodeId, slave.getHost(), slave.getPort(), e.getMessage(), e);
                replicationState.setSlaveConnected(slaveNodeId, false);
                return null;
            }
        });
    }
    
    /**
     * 注册从节点
     */
    public void registerSlave(String slaveNodeId, String host, int port) {
        replicationState.registerSlave(slaveNodeId, host, port);
        log.info("Registered slave node: {} at {}:{}", slaveNodeId, host, port);
    }
    
    /**
     * 注销从节点
     */
    public void unregisterSlave(String slaveNodeId) {
        TcpReplicationClient client = clients.remove(slaveNodeId);
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting TCP client for slave {}", slaveNodeId, e);
            }
        }
        replicationState.unregisterSlave(slaveNodeId);
    }
    
    /**
     * 处理从节点心跳
     */
    public void handleSlaveHeartbeat(String slaveNodeId) {
        replicationState.updateSlaveHeartbeat(slaveNodeId);
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
        
        // 发送剩余批次
        synchronized (currentBatch) {
            if (!currentBatch.isEmpty()) {
                sendBatchToSlaves();
            }
        }
        
        // 关闭所有客户端连接
        for (Map.Entry<String, TcpReplicationClient> entry : clients.entrySet()) {
            try {
                entry.getValue().disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting TCP client for slave {}", entry.getKey(), e);
            }
        }
        clients.clear();
        
        // 关闭线程池
        if (replicationExecutor != null) {
            replicationExecutor.shutdown();
            try {
                if (!replicationExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    replicationExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                replicationExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // 打印性能统计
        logPerformanceStats();
    }
    
    /**
     * 打印性能统计
     */
    private void logPerformanceStats() {
        long totalEvents = totalEventsProcessed.get();
        long totalBatches = totalBatchesSent.get();
        long totalTime = totalSendTime.get();
        
        if (totalBatches > 0) {
            double avgBatchSize = (double) totalEvents / totalBatches;
            double avgSendTime = (double) totalTime / totalBatches;
            double eventsPerSecond = totalEvents * 1000.0 / Math.max(totalTime, 1);
            
            log.info("ReplicationHandler Performance Stats - Events: {}, Batches: {}, Avg Batch Size: {:.2f}, " +
                    "Avg Send Time: {:.2f}ms, Events/sec: {:.2f}", 
                    totalEvents, totalBatches, avgBatchSize, avgSendTime, eventsPerSecond);
        }
    }
    
    /**
     * 设置TCP复制服务器引用
     */
    public void setTcpReplicationServer(TcpReplicationServer tcpReplicationServer) {
        this.tcpReplicationServer = tcpReplicationServer;
        log.info("TcpReplicationServer reference set in ReplicationHandler");
    }
    
    /**
     * 获取当前批次ID（供BarrierHandler使用）
     */
    public long getCurrentBatchId() {
        return currentBatchId;
    }
    
    /**
     * 获取性能统计信息
     */
    public PerformanceStats getPerformanceStats() {
        return new PerformanceStats(
            totalEventsProcessed.get(),
            totalBatchesSent.get(),
            totalSendTime.get(),
            clients.size()
        );
    }
    
    /**
     * 性能统计数据结构
     */
    public static class PerformanceStats {
        public final long totalEventsProcessed;
        public final long totalBatchesSent;
        public final long totalSendTimeMs;
        public final int activeSlaveCount;
        
        public PerformanceStats(long totalEventsProcessed, long totalBatchesSent, 
                               long totalSendTimeMs, int activeSlaveCount) {
            this.totalEventsProcessed = totalEventsProcessed;
            this.totalBatchesSent = totalBatchesSent;
            this.totalSendTimeMs = totalSendTimeMs;
            this.activeSlaveCount = activeSlaveCount;
        }
        
        @Override
        public String toString() {
            return String.format("PerformanceStats{events=%d, batches=%d, avgSendTime=%.2fms, slaves=%d}",
                    totalEventsProcessed, totalBatchesSent, 
                    totalBatchesSent > 0 ? (double) totalSendTimeMs / totalBatchesSent : 0.0,
                    activeSlaveCount);
        }
    }
}