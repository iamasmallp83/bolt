package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP复制客户端 - 负责与从节点的TCP通信
 * 替代gRPC客户端，直接发送NexusWrapper数据
 */
@Slf4j
public class TcpReplicationClient {
    
    private final String host;
    private final int port;
    private final String slaveNodeId;
    private Socket socket;
    private DataOutputStream outputStream;
    private DataInputStream inputStream;
    private volatile boolean connected = false;
    
    // Connection resilience fields
    private final AtomicBoolean shouldReconnect = new AtomicBoolean(true);
    private final AtomicLong reconnectAttempts = new AtomicLong(0);
    private final AtomicLong lastSuccessfulHeartbeat = new AtomicLong(System.currentTimeMillis());
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private volatile boolean shutdown = false;
    
    // Configuration constants
    private static final long MAX_RECONNECT_ATTEMPTS = 10;
    private static final long INITIAL_RECONNECT_DELAY_MS = 1000;
    private static final long MAX_RECONNECT_DELAY_MS = 30000;
    private static final long HEARTBEAT_INTERVAL_MS = 10000;
    private static final long HEARTBEAT_TIMEOUT_MS = 30000;
    
    // TCP协议常量
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;
    
    public TcpReplicationClient(String host, int port, String slaveNodeId) {
        this.host = host;
        this.port = port;
        this.slaveNodeId = slaveNodeId;
    }
    
    /**
     * 连接到从节点
     */
    public void connect() {
        log.info("Attempting to connect to slave at {}:{} for slave {}", host, port, slaveNodeId);
        
        try {
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true); // 禁用Nagle算法，减少延迟
            socket.setKeepAlive(true); // 启用TCP keep-alive
            socket.setSoTimeout(30000); // 30秒超时
            
            outputStream = new DataOutputStream(socket.getOutputStream());
            inputStream = new DataInputStream(socket.getInputStream());
            
            connected = true;
            reconnectAttempts.set(0); // 重置重连计数
            lastSuccessfulHeartbeat.set(System.currentTimeMillis());
            
            // 启动心跳和连接监控
            startHeartbeat();
            startConnectionMonitoring();
            
            log.info("Successfully connected to slave at {}:{} for slave {}", host, port, slaveNodeId);
        } catch (Exception e) {
            log.error("Failed to connect to slave at {}:{} for slave {}: {}", host, port, slaveNodeId, e.getMessage(), e);
            handleConnectionError(e);
            throw new RuntimeException("Connection failed", e);
        }
    }
    
    /**
     * 处理连接错误
     */
    private void handleConnectionError(Throwable t) {
        connected = false;
        log.error("Connection error for slave {}: {}", slaveNodeId, t.getMessage());
        scheduleReconnect();
    }
    
    /**
     * 安排重连
     */
    private void scheduleReconnect() {
        if (shutdown || !shouldReconnect.get()) {
            return;
        }
        
        long attempts = reconnectAttempts.incrementAndGet();
        if (attempts > MAX_RECONNECT_ATTEMPTS) {
            log.error("Max reconnection attempts ({}) exceeded for slave {}", MAX_RECONNECT_ATTEMPTS, slaveNodeId);
            shouldReconnect.set(false);
            return;
        }
        
        // 指数退避重连延迟
        long delay = Math.min(INITIAL_RECONNECT_DELAY_MS * (1L << (attempts - 1)), MAX_RECONNECT_DELAY_MS);
        
        log.info("Scheduling reconnection attempt {} for slave {} in {} ms", attempts, slaveNodeId, delay);
        
        scheduler.schedule(() -> {
            if (!shutdown && shouldReconnect.get()) {
                try {
                    disconnect();
                    connect();
                } catch (Exception e) {
                    log.error("Reconnection attempt {} failed for slave {}", attempts, slaveNodeId, e);
                    scheduleReconnect();
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动心跳机制
     */
    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            if (connected && !shutdown) {
                try {
                    sendHeartbeat();
                    lastSuccessfulHeartbeat.set(System.currentTimeMillis());
                } catch (Exception e) {
                    log.warn("Heartbeat failed for slave {}", slaveNodeId, e);
                    handleConnectionError(e);
                }
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动连接监控
     */
    private void startConnectionMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!shutdown && connected) {
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastSuccessfulHeartbeat.get();
                if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                    log.warn("Heartbeat timeout for slave {} ({} ms since last heartbeat)", 
                            slaveNodeId, timeSinceLastHeartbeat);
                    handleConnectionError(new RuntimeException("Heartbeat timeout"));
                }
            }
        }, HEARTBEAT_TIMEOUT_MS / 2, HEARTBEAT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 发送批次到从节点
     */
    public void sendBatch(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        if (!isConnected()) {
            log.warn("无法发送批次 - 客户端未连接, batchId: {}, slave: {}", batchId, slaveNodeId);
            scheduleReconnect();
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            log.info("开始发送批次到从节点 - batchId: {}, eventCount: {}, sequences: {}-{}, slave: {}", 
                    batchId, events.size(), sequenceStart, sequenceEnd, slaveNodeId);
            
            // 发送批次头
            sendBatchHeader(batchId, events.size(), sequenceStart, sequenceEnd);
            
            // 发送每个NexusWrapper
            log.debug("开始发送批次中的 {} 个事件 - batchId: {}, slave: {}", events.size(), batchId, slaveNodeId);
            for (int i = 0; i < events.size(); i++) {
                NexusWrapper wrapper = events.get(i);
                log.debug("发送第 {}/{} 个事件 - batchId: {}, eventId: {}, slave: {}", 
                        i+1, events.size(), batchId, wrapper.getId(), slaveNodeId);
                sendNexusWrapper(wrapper);
            }
            
            // 发送批次结束标记
            sendBatchEnd();
            
            log.info("批次发送成功 - batchId: {}, eventCount: {}, slave: {}", batchId, events.size(), slaveNodeId);
            lastSuccessfulHeartbeat.set(System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("发送批次失败 - batchId: {}, eventCount: {}, slave: {}", 
                    batchId, events.size(), slaveNodeId, e);
            handleConnectionError(e);
            throw new RuntimeException("Failed to send batch", e);
        }
    }
    
    /**
     * 发送批次头
     */
    private void sendBatchHeader(long batchId, int eventCount, long sequenceStart, long sequenceEnd) throws IOException {
        log.debug("开始发送批次头 - batchId: {}, eventCount: {}, sequences: {}-{}, slave: {}", 
                batchId, eventCount, sequenceStart, sequenceEnd, slaveNodeId);
        
        // 协议格式：
        // Magic Number (4 bytes) + Version (4 bytes) + Message Type (4 bytes) + 
        // Batch ID (8 bytes) + Event Count (4 bytes) + Sequence Start (8 bytes) + Sequence End (8 bytes)
        
        ByteBuffer header = ByteBuffer.allocate(40);
        header.putInt(MAGIC_NUMBER);
        header.putInt(VERSION);
        header.putInt(1); // Message Type: 1 = Batch
        header.putLong(batchId);
        header.putInt(eventCount);
        header.putLong(sequenceStart);
        header.putLong(sequenceEnd);
        
        outputStream.write(header.array());
        outputStream.flush();
        
        log.debug("批次头发送成功 - batchId: {}, eventCount: {}, slave: {}", batchId, eventCount, slaveNodeId);
    }
    
    /**
     * 发送NexusWrapper数据
     */
    private void sendNexusWrapper(NexusWrapper wrapper) throws IOException {
        log.debug("开始发送NexusWrapper - id: {}, partition: {}, slave: {}", 
                wrapper.getId(), wrapper.getPartition(), slaveNodeId);
        
        ByteBuf buffer = wrapper.getBuffer();
        int readableBytes = buffer.readableBytes();
        
        log.debug("NexusWrapper数据大小 - id: {}, dataLength: {}, slave: {}", 
                wrapper.getId(), readableBytes, slaveNodeId);
        
        // 发送NexusWrapper头：ID (8 bytes) + Partition (4 bytes) + Data Length (4 bytes)
        ByteBuffer wrapperHeader = ByteBuffer.allocate(16);
        wrapperHeader.putLong(wrapper.getId());
        wrapperHeader.putInt(wrapper.getPartition());
        wrapperHeader.putInt(readableBytes);
        
        outputStream.write(wrapperHeader.array());
        
        // 发送实际数据
        if (readableBytes > 0) {
            byte[] data = new byte[readableBytes];
            buffer.getBytes(buffer.readerIndex(), data);
            outputStream.write(data);
            log.debug("NexusWrapper数据发送完成 - id: {}, dataLength: {}, slave: {}", 
                    wrapper.getId(), readableBytes, slaveNodeId);
        } else {
            log.debug("NexusWrapper无数据内容 - id: {}, slave: {}", wrapper.getId(), slaveNodeId);
        }
        
        outputStream.flush();
        log.debug("NexusWrapper发送成功 - id: {}, slave: {}", wrapper.getId(), slaveNodeId);
    }
    
    /**
     * 发送批次结束标记
     */
    private void sendBatchEnd() throws IOException {
        log.debug("开始发送批次结束标记 - slave: {}", slaveNodeId);
        
        ByteBuffer endMarker = ByteBuffer.allocate(12);
        endMarker.putInt(MAGIC_NUMBER);
        endMarker.putInt(VERSION);
        endMarker.putInt(2); // Message Type: 2 = Batch End
        
        outputStream.write(endMarker.array());
        outputStream.flush();
        
        log.debug("批次结束标记发送成功 - slave: {}", slaveNodeId);
    }
    
    /**
     * 发送心跳
     */
    public void sendHeartbeat() throws IOException {
        if (!isConnected()) {
            log.debug("无法发送心跳 - 连接未建立, slave: {}", slaveNodeId);
            return;
        }
        
        log.debug("开始发送心跳 - slave: {}", slaveNodeId);
        
        ByteBuffer heartbeat = ByteBuffer.allocate(16);
        heartbeat.putInt(MAGIC_NUMBER);
        heartbeat.putInt(VERSION);
        heartbeat.putInt(3); // Message Type: 3 = Heartbeat
        heartbeat.putLong(System.currentTimeMillis());
        
        outputStream.write(heartbeat.array());
        outputStream.flush();
        
        log.debug("心跳发送成功 - slave: {}", slaveNodeId);
    }
    
    /**
     * 发送批次确认
     */
    public void sendBatchAcknowledgment(long batchId, long processedSequence) {
        if (!isConnected()) {
            log.warn("Cannot send batch acknowledgment - client not connected");
            return;
        }
        
        try {
            ByteBuffer ack = ByteBuffer.allocate(28);
            ack.putInt(MAGIC_NUMBER);
            ack.putInt(VERSION);
            ack.putInt(4); // Message Type: 4 = Batch Ack
            ack.putLong(batchId);
            ack.putLong(processedSequence);
            ack.putLong(System.currentTimeMillis());
            
            outputStream.write(ack.array());
            outputStream.flush();
            
            log.debug("Batch acknowledgment sent for batch {}", batchId);
        } catch (Exception e) {
            log.error("Failed to send batch acknowledgment for batch {}", batchId, e);
            handleConnectionError(e);
        }
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        log.info("Attempting to disconnect from slave at {}:{} for slave {}", host, port, slaveNodeId);
        
        shutdown = true;
        shouldReconnect.set(false);
        connected = false;
        
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (Exception e) {
                log.warn("Error closing output stream", e);
            }
            outputStream = null;
        }
        
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (Exception e) {
                log.warn("Error closing input stream", e);
            }
            inputStream = null;
        }
        
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
                log.info("Successfully disconnected from slave at {}:{} for slave {}", host, port, slaveNodeId);
            } catch (Exception e) {
                log.warn("Error closing socket", e);
            }
        } else {
            log.debug("Socket already closed or null for slave {}", slaveNodeId);
        }
        
        // 关闭调度器
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }
    
    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed() && socket.isConnected();
    }
    
    /**
     * 获取重连状态
     */
    public boolean shouldReconnect() {
        return shouldReconnect.get() && !shutdown;
    }
    
    /**
     * 获取重连尝试次数
     */
    public long getReconnectAttempts() {
        return reconnectAttempts.get();
    }
    
    /**
     * 获取最后成功心跳时间
     */
    public long getLastSuccessfulHeartbeat() {
        return lastSuccessfulHeartbeat.get();
    }
    
    /**
     * 获取从节点ID
     */
    public String getSlaveNodeId() {
        return slaveNodeId;
    }
}
