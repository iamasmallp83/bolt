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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制处理器 - 负责将事件复制到从节点
 * 实现批量确认机制，确保强一致性
 * 整合了客户端管理功能，无需单独的ReplicationClientManager
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
    
    // 客户端连接管理
    private final Map<String, ReplicationClient> clients = new ConcurrentHashMap<>();
    
    public ReplicationHandler(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;
        this.currentBatch = new ArrayList<>(config.batchSize());
        
        log.info("ReplicationHandler initialized - batchSize: {}, replicationPort: {}", 
                config.batchSize(), config.replicationPort());
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        // 早期返回检查
        if (wrapper.getBuffer() == null || wrapper.getBuffer().readableBytes() == 0 || wrapper.getId() == -1) {
            this.sequence.set(sequence);
            return;
        }
        
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
                sendBatchToSlaves();
            }
        }
        
        this.sequence.set(sequence);
    }
    
    /**
     * 发送批次到从节点
     */
    private void sendBatchToSlaves() {
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
                config.batchTimeoutMs(), sequenceStart, sequenceEnd);
        
        log.info("Sending batch {} with {} events, sequences {}-{}", 
                tracker.getBatchId(), batchToSend.size(), sequenceStart, sequenceEnd);
        
        // 发送到所有从节点
        sendBatchToSlaves(tracker.getBatchId(), batchToSend, sequenceStart, sequenceEnd);
        
        // 等待确认（非阻塞方式）
        waitForBatchAcknowledgment(tracker);
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
     * 发送批次到所有从节点
     */
    private void sendBatchToSlaves(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        Map<String, ReplicationState.SlaveNode> slaves = replicationState.getAllSlaves();
        
        log.info("Attempting to send batch {} to {} slaves", batchId, slaves.size());
        
        if (slaves.isEmpty()) {
            log.warn("No slaves available for batch {}", batchId);
            return;
        }
        
        for (Map.Entry<String, ReplicationState.SlaveNode> entry : slaves.entrySet()) {
            String slaveNodeId = entry.getKey();
            ReplicationState.SlaveNode slave = entry.getValue();
            
            log.debug("Processing slave {}: host={}, port={}, healthy={}", 
                    slaveNodeId, slave.getHost(), slave.getPort(), slave.isHealthy(30000));
            
            if (slave.isHealthy(30000)) { // 30秒超时
                ReplicationClient client = getOrCreateClient(slaveNodeId, slave);
                if (client != null) {
                    try {
                        log.debug("Sending batch {} to slave {} ({}:{})", 
                                batchId, slaveNodeId, slave.getHost(), slave.getPort());
                        client.sendBatch(batchId, events, sequenceStart, sequenceEnd);
                        log.debug("Successfully sent batch {} to slave {}", batchId, slaveNodeId);
                    } catch (Exception e) {
                        log.error("Failed to send batch {} to slave {} ({}:{}): {}", 
                                batchId, slaveNodeId, slave.getHost(), slave.getPort(), e.getMessage(), e);
                        replicationState.setSlaveConnected(slaveNodeId, false);
                    }
                } else {
                    log.warn("Failed to create client for slave {} ({}:{})", 
                            slaveNodeId, slave.getHost(), slave.getPort());
                }
            } else {
                log.debug("Skipping unhealthy slave {} for batch {}", slaveNodeId, batchId);
            }
        }
    }
    
    /**
     * 获取或创建复制客户端
     */
    private ReplicationClient getOrCreateClient(String slaveNodeId, ReplicationState.SlaveNode slave) {
        log.debug("Getting or creating client for slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
        
        ReplicationClient existingClient = clients.get(slaveNodeId);
        if (existingClient != null) {
            log.debug("Using existing client for slave {}", slaveNodeId);
            return existingClient;
        }
        
        log.info("Creating new replication client for slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
        
        return clients.computeIfAbsent(slaveNodeId, id -> {
            try {
                ReplicationClient client = new ReplicationClient(slave.getHost(), slave.getPort(), slaveNodeId);
                log.debug("Connecting to slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
                client.connect();
                replicationState.setSlaveConnected(slaveNodeId, true);
                log.info("Successfully created and connected replication client for slave {} at {}:{}", 
                        slaveNodeId, slave.getHost(), slave.getPort());
                return client;
            } catch (Exception e) {
                log.error("Failed to create replication client for slave {} at {}:{}: {}", 
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
        ReplicationClient client = clients.remove(slaveNodeId);
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting client for slave {}", slaveNodeId, e);
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
        for (Map.Entry<String, ReplicationClient> entry : clients.entrySet()) {
            try {
                entry.getValue().disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting client for slave {}", entry.getKey(), e);
            }
        }
        clients.clear();
    }
}