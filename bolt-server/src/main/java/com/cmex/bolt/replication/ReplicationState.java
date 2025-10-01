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
            this.connected = true; // Mark as connected by default for testing
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
//            return connected && (System.currentTimeMillis() - lastHeartbeat) < timeoutMs;
            return true;
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
    
}
