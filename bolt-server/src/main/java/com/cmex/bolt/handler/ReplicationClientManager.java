package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 复制客户端管理器
 * 负责管理与从节点的连接和通信
 */
@Slf4j
public class ReplicationClientManager {
    
    private final BoltConfig config;
    private final ReplicationState replicationState;
    private final Map<String, ReplicationClient> clients = new ConcurrentHashMap<>();
    
    public ReplicationClientManager(BoltConfig config, ReplicationState replicationState) {
        this.config = config;
        this.replicationState = replicationState;
    }
    
    /**
     * 发送批次到所有从节点
     */
    public void sendBatchToSlaves(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        Map<String, ReplicationState.SlaveNode> slaves = replicationState.getAllSlaves();
        
        for (Map.Entry<String, ReplicationState.SlaveNode> entry : slaves.entrySet()) {
            String slaveNodeId = entry.getKey();
            ReplicationState.SlaveNode slave = entry.getValue();
            
            if (slave.isHealthy(30000)) { // 30秒超时
                ReplicationClient client = getOrCreateClient(slaveNodeId, slave);
                if (client != null) {
                    try {
                        client.sendBatch(batchId, events, sequenceStart, sequenceEnd);
                    } catch (Exception e) {
                        log.error("Failed to send batch {} to slave {}", batchId, slaveNodeId, e);
                        replicationState.setSlaveConnected(slaveNodeId, false);
                    }
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
        return clients.computeIfAbsent(slaveNodeId, id -> {
            try {
                ReplicationClient client = new ReplicationClient(slave.getHost(), slave.getPort(), slaveNodeId);
                client.connect();
                replicationState.setSlaveConnected(slaveNodeId, true);
                log.info("Created replication client for slave {} at {}:{}", slaveNodeId, slave.getHost(), slave.getPort());
                return client;
            } catch (Exception e) {
                log.error("Failed to create replication client for slave {}", slaveNodeId, e);
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
    
    /**
     * 处理从节点确认
     */
    public void handleSlaveAcknowledgment(long batchId, String slaveNodeId) {
        replicationState.acknowledgeBatch(batchId, slaveNodeId);
    }
    
    /**
     * 启动客户端管理器
     */
    public void start() {
        log.info("ReplicationClientManager started");
    }
    
    /**
     * 关闭客户端管理器
     */
    public void shutdown() {
        log.info("Shutting down ReplicationClientManager");
        
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
