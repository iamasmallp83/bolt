package com.cmex.bolt.replication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制状态管理器
 * 负责跟踪从节点状态和批次确认状态
 */
@Slf4j
public class ReplicationState {
    
    private final Map<String, SlaveNode> slaveNodes = new ConcurrentHashMap<>();
    private final Map<Long, BatchAckTracker> batchTrackers = new ConcurrentHashMap<>();
    private final AtomicLong nextBatchId = new AtomicLong(1);
    
    /**
     * 从节点信息
     */
    @Getter
    public static class SlaveNode {
        private final String nodeId;
        private final String host;
        private final int port;
        private volatile boolean connected;
        private volatile long lastHeartbeat;
        private volatile long lastAcknowledgedSequence;
        
        public SlaveNode(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.connected = false;
            this.lastHeartbeat = System.currentTimeMillis();
            this.lastAcknowledgedSequence = -1;
        }
        
        public void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public void updateAcknowledgedSequence(long sequence) {
            this.lastAcknowledgedSequence = sequence;
        }
        
        public boolean isHealthy(long timeoutMs) {
            return connected && (System.currentTimeMillis() - lastHeartbeat) < timeoutMs;
        }
    }
    
    /**
     * 批次确认跟踪器
     */
    @Getter
    public static class BatchAckTracker {
        private final long batchId;
        private final Set<String> pendingSlaves;
        private final Set<String> acknowledgedSlaves;
        private final long timestamp;
        private final int timeoutMs;
        private final long sequenceStart;
        private final long sequenceEnd;
        
        public BatchAckTracker(long batchId, Set<String> slaveNodeIds, int timeoutMs, long sequenceStart, long sequenceEnd) {
            this.batchId = batchId;
            this.pendingSlaves = ConcurrentHashMap.newKeySet();
            this.pendingSlaves.addAll(slaveNodeIds);
            this.acknowledgedSlaves = ConcurrentHashMap.newKeySet();
            this.timestamp = System.currentTimeMillis();
            this.timeoutMs = timeoutMs;
            this.sequenceStart = sequenceStart;
            this.sequenceEnd = sequenceEnd;
        }
        
        public boolean acknowledge(String slaveNodeId) {
            if (pendingSlaves.remove(slaveNodeId)) {
                acknowledgedSlaves.add(slaveNodeId);
                log.debug("Slave {} acknowledged batch {}", slaveNodeId, batchId);
                return true;
            }
            return false;
        }
        
        public boolean isAllAcknowledged() {
            return pendingSlaves.isEmpty();
        }
        
        public boolean isTimeout() {
            return (System.currentTimeMillis() - timestamp) > timeoutMs;
        }
        
        public Set<String> getTimeoutSlaves() {
            return Set.copyOf(pendingSlaves);
        }
        
        public int getAcknowledgedCount() {
            return acknowledgedSlaves.size();
        }
        
        public int getPendingCount() {
            return pendingSlaves.size();
        }
    }
    
    /**
     * 注册从节点
     */
    public void registerSlave(String nodeId, String host, int port) {
        SlaveNode slave = new SlaveNode(nodeId, host, port);
        slaveNodes.put(nodeId, slave);
        log.info("Registered slave node: {} at {}:{}", nodeId, host, port);
    }
    
    /**
     * 注销从节点
     */
    public void unregisterSlave(String nodeId) {
        SlaveNode removed = slaveNodes.remove(nodeId);
        if (removed != null) {
            log.info("Unregistered slave node: {}", nodeId);
        }
    }
    
    /**
     * 更新从节点心跳
     */
    public void updateSlaveHeartbeat(String nodeId) {
        SlaveNode slave = slaveNodes.get(nodeId);
        if (slave != null) {
            slave.updateHeartbeat();
        }
    }
    
    /**
     * 设置从节点连接状态
     */
    public void setSlaveConnected(String nodeId, boolean connected) {
        SlaveNode slave = slaveNodes.get(nodeId);
        if (slave != null) {
            slave.connected = connected;
            log.info("Slave node {} connection status: {}", nodeId, connected);
        }
    }
    
    /**
     * 创建新的批次跟踪器
     */
    public BatchAckTracker createBatchTracker(int timeoutMs, long sequenceStart, long sequenceEnd) {
        long batchId = nextBatchId.getAndIncrement();
        Set<String> healthySlaves = getHealthySlaveIds();
        
        BatchAckTracker tracker = new BatchAckTracker(batchId, healthySlaves, timeoutMs, sequenceStart, sequenceEnd);
        batchTrackers.put(batchId, tracker);
        
        log.debug("Created batch tracker {} for {} slaves, sequences {}-{}", 
                batchId, healthySlaves.size(), sequenceStart, sequenceEnd);
        
        return tracker;
    }
    
    /**
     * 确认批次
     */
    public boolean acknowledgeBatch(long batchId, String slaveNodeId) {
        BatchAckTracker tracker = batchTrackers.get(batchId);
        if (tracker == null) {
            log.warn("Received ack for unknown batch {} from slave {}", batchId, slaveNodeId);
            return false;
        }
        
        boolean acknowledged = tracker.acknowledge(slaveNodeId);
        if (acknowledged) {
            // 更新从节点的最后确认序列号
            SlaveNode slave = slaveNodes.get(slaveNodeId);
            if (slave != null) {
                slave.updateAcknowledgedSequence(tracker.getSequenceEnd());
            }
            
            // 如果所有从节点都已确认，清理跟踪器
            if (tracker.isAllAcknowledged()) {
                batchTrackers.remove(batchId);
                log.debug("Batch {} fully acknowledged by all slaves", batchId);
            }
        }
        
        return acknowledged;
    }
    
    /**
     * 获取健康的从节点ID列表
     */
    public Set<String> getHealthySlaveIds() {
        return slaveNodes.entrySet().stream()
                .filter(entry -> entry.getValue().isHealthy(30000)) // 30秒超时
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }
    
    /**
     * 获取所有从节点
     */
    public Map<String, SlaveNode> getAllSlaves() {
        return Map.copyOf(slaveNodes);
    }
    
    /**
     * 获取超时的批次跟踪器
     */
    public Map<Long, BatchAckTracker> getTimeoutBatchTrackers() {
        return batchTrackers.entrySet().stream()
                .filter(entry -> entry.getValue().isTimeout())
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    /**
     * 清理超时的批次跟踪器
     */
    public void cleanupTimeoutTrackers() {
        Map<Long, BatchAckTracker> timeoutTrackers = getTimeoutBatchTrackers();
        for (Map.Entry<Long, BatchAckTracker> entry : timeoutTrackers.entrySet()) {
            long batchId = entry.getKey();
            BatchAckTracker tracker = entry.getValue();
            
            batchTrackers.remove(batchId);
            log.warn("Cleaned up timeout batch tracker {} with {} pending slaves", 
                    batchId, tracker.getPendingCount());
        }
    }
    
    /**
     * 获取统计信息
     */
    public ReplicationStats getStats() {
        int totalSlaves = slaveNodes.size();
        int healthySlaves = (int) slaveNodes.values().stream()
                .mapToInt(slave -> slave.isHealthy(30000) ? 1 : 0)
                .sum();
        int activeBatches = batchTrackers.size();
        
        return new ReplicationStats(totalSlaves, healthySlaves, activeBatches);
    }
    
    /**
     * 复制统计信息
     */
    @Getter
    public static class ReplicationStats {
        private final int totalSlaves;
        private final int healthySlaves;
        private final int activeBatches;
        
        public ReplicationStats(int totalSlaves, int healthySlaves, int activeBatches) {
            this.totalSlaves = totalSlaves;
            this.healthySlaves = healthySlaves;
            this.activeBatches = activeBatches;
        }
    }
}
