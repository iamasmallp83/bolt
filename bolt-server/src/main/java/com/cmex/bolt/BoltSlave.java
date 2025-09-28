package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.handler.TcpSlaveClient;
import com.lmax.disruptor.RingBuffer;
import com.cmex.bolt.core.NexusWrapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * Bolt从节点 - 负责连接到主节点并接收复制数据
 */
@Slf4j
public class BoltSlave extends BoltBase {
    
    private TcpSlaveClient slaveClient;
    
    public BoltSlave(BoltConfig config) {
        super(config);
        
        // 验证从节点配置
        if (config.isMaster()) {
            throw new IllegalArgumentException("BoltSlave requires isMaster=false in config");
        }
        
        log.info("BoltSlave initialized - master host: {}, master port: {}, replication enabled: {}", 
                config.masterHost(), config.masterPort(), config.enableReplication());
    }
    
    @Override
    protected void startNodeSpecificServices() throws IOException, InterruptedException {
        // 启动从节点客户端（如果启用复制）
        if (config.enableReplication()) {
            log.info("Starting slave client connection to master at {}:{}", 
                    config.masterHost(), config.masterPort());
            
            // 获取sequencer ring buffer用于接收复制数据
            RingBuffer<NexusWrapper> sequencerRingBuffer = envoyServer.getSequencerRingBuffer();
            
            // 创建从节点客户端
            String slaveNodeId = generateSlaveNodeId();
            this.slaveClient = new TcpSlaveClient(
                    config.masterHost(), 
                    config.masterPort(), 
                    slaveNodeId, 
                    sequencerRingBuffer
            );
            
            try {
                slaveClient.connect();
                log.info("Slave client connected successfully to master at {}:{}", 
                        config.masterHost(), config.masterPort());
            } catch (Exception e) {
                log.error("Failed to connect slave client to master at {}:{}: {}", 
                        config.masterHost(), config.masterPort(), e.getMessage(), e);
                throw new RuntimeException("Failed to connect to master", e);
            }
        } else {
            log.info("Slave client not started - replication disabled");
        }
    }
    
    @Override
    protected void stopNodeSpecificServices() {
        if (slaveClient != null) {
            log.info("Disconnecting slave client from master...");
            slaveClient.disconnect();
            log.info("Slave client disconnected from master");
        }
    }
    
    @Override
    public boolean isRunning() {
        boolean baseRunning = super.isRunning();
        boolean slaveConnected = slaveClient == null || slaveClient.isConnected();
        return baseRunning && slaveConnected;
    }
    
    /**
     * 生成从节点ID
     */
    private String generateSlaveNodeId() {
        // 使用主机名和端口生成唯一的从节点ID
        String hostname = System.getProperty("user.name", "unknown");
        return String.format("slave-%s-%d-%d", hostname, config.port(), System.currentTimeMillis());
    }
    
    /**
     * 获取从节点客户端
     */
    public TcpSlaveClient getSlaveClient() {
        return slaveClient;
    }
    
    /**
     * 检查从节点是否连接到主节点
     */
    public boolean isConnectedToMaster() {
        return slaveClient != null && slaveClient.isConnected();
    }
    
    /**
     * 获取从节点ID
     */
    public String getSlaveNodeId() {
        return slaveClient != null ? slaveClient.getSlaveNodeId() : null;
    }
    
    /**
     * 获取重连状态
     */
    public boolean shouldReconnect() {
        return slaveClient != null && slaveClient.shouldReconnect();
    }
    
    /**
     * 获取重连尝试次数
     */
    public long getReconnectAttempts() {
        return slaveClient != null ? slaveClient.getReconnectAttempts() : 0;
    }
}
