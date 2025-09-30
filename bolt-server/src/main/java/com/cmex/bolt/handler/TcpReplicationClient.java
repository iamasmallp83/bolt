package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.RingBuffer;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
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
 * TCP复制客户端 - 支持双向通信
 * 可以作为主节点客户端连接从节点，也可以作为从节点客户端连接主节点
 */
@Slf4j
public class TcpReplicationClient {
    
    private final String host;
    private final int port;
    private final String nodeId;
    private final boolean isSlaveMode; // true=从节点模式，false=主节点模式
    private final RingBuffer<NexusWrapper> sequencerRingBuffer; // 从节点模式使用
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
    
    /**
     * 主节点模式构造函数（连接到从节点）
     */
    public TcpReplicationClient(String host, int port, String slaveNodeId) {
        this.host = host;
        this.port = port;
        this.nodeId = slaveNodeId;
        this.isSlaveMode = false;
        this.sequencerRingBuffer = null;
    }
    
    /**
     * 从节点模式构造函数（连接到主节点）
     */
    public TcpReplicationClient(String masterHost, int masterPort, String slaveNodeId, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.host = masterHost;
        this.port = masterPort;
        this.nodeId = slaveNodeId;
        this.isSlaveMode = true;
        this.sequencerRingBuffer = sequencerRingBuffer;
    }
    
    /**
     * 连接到目标节点
     */
    public void connect() {
        String targetType = isSlaveMode ? "master" : "slave";
        log.info("Attempting to connect to {} at {}:{} for node {}", targetType, host, port, nodeId);
        
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
            
            // 从节点模式启动消息接收线程
            if (isSlaveMode) {
                startMessageReceiver();
            }
            
            log.info("Successfully connected to {} at {}:{} for node {}", targetType, host, port, nodeId);
        } catch (Exception e) {
            log.error("Failed to connect to {} at {}:{} for node {}: {}", targetType, host, port, nodeId, e.getMessage(), e);
            handleConnectionError(e);
            throw new RuntimeException("Connection failed", e);
        }
    }
    
    /**
     * 处理连接错误
     */
    private void handleConnectionError(Throwable t) {
        connected = false;
        log.error("Connection error for node {}: {}", nodeId, t.getMessage());
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
            log.error("Max reconnection attempts ({}) exceeded for node {}", MAX_RECONNECT_ATTEMPTS, nodeId);
            shouldReconnect.set(false);
            return;
        }
        
        // 指数退避重连延迟
        long delay = Math.min(INITIAL_RECONNECT_DELAY_MS * (1L << (attempts - 1)), MAX_RECONNECT_DELAY_MS);
        
        log.info("Scheduling reconnection attempt {} for node {} in {} ms", attempts, nodeId, delay);
        
        scheduler.schedule(() -> {
            if (!shutdown && shouldReconnect.get()) {
                try {
                    disconnect();
                    connect();
                } catch (Exception e) {
                    log.error("Reconnection attempt {} failed for node {}", attempts, nodeId, e);
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
                    log.warn("Heartbeat failed for node {}", nodeId, e);
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
                    log.warn("Heartbeat timeout for node {} ({} ms since last heartbeat)", 
                            nodeId, timeSinceLastHeartbeat);
                    handleConnectionError(new RuntimeException("Heartbeat timeout"));
                }
            }
        }, HEARTBEAT_TIMEOUT_MS / 2, HEARTBEAT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动消息接收线程（从节点模式）
     */
    private void startMessageReceiver() {
        scheduler.submit(() -> {
            while (connected && !shutdown) {
                try {
                    processMessage();
                } catch (IOException e) {
                    if (connected) {
                        log.warn("Error processing message from master", e);
                        handleConnectionError(e);
                    }
                    break;
                } catch (Exception e) {
                    log.error("Unexpected error processing message", e);
                    handleConnectionError(e);
                    break;
                }
            }
        });
    }
    
    /**
     * 处理来自主节点的消息（从节点模式）
     */
    private void processMessage() throws IOException {
        log.debug("开始读取来自主节点的消息 - node: {}", nodeId);
        
        // 读取消息头
        int magic = inputStream.readInt();
        log.debug("读取到magic number: {} (期望: {}) - node: {}", 
                Integer.toHexString(magic), Integer.toHexString(MAGIC_NUMBER), nodeId);
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }
        
        int version = inputStream.readInt();
        log.debug("读取到version: {} (期望: {}) - node: {}", version, VERSION, nodeId);
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        int messageType = inputStream.readInt();
        log.debug("读取到message type: {} - node: {}", messageType, nodeId);
        
        switch (messageType) {
            case 1 -> processNexusWrapperMessage(); // 新协议：直接NexusWrapper
            case 2 -> processBatchMessage(); // 旧协议：Batch
            case 3 -> processBatchEndMessage(); // 旧协议：Batch End
            case 4 -> processHeartbeatMessage(); // Heartbeat
            default -> {
                log.warn("Unknown message type: {} - node: {}", messageType, nodeId);
                throw new IOException("Unknown message type: " + messageType);
            }
        }
    }
    
    /**
     * 处理新的NexusWrapper消息（新协议）
     */
    private void processNexusWrapperMessage() throws IOException {
        log.debug("处理新协议NexusWrapper消息 - node: {}", nodeId);
        
        // 读取NexusWrapper头
        long id = inputStream.readLong();
        int combinedPartitionAndEventType = inputStream.readInt();
        int dataLength = inputStream.readInt();
        
        log.debug("读取NexusWrapper头信息 - id: {}, combinedPartitionAndEventType: {}, dataLength: {}, node: {}", 
                id, combinedPartitionAndEventType, dataLength, nodeId);
        
        // 创建NexusWrapper
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, dataLength);
        wrapper.setId(id);
        wrapper.setPartitionByCombined(combinedPartitionAndEventType);
        wrapper.setEventType(NexusWrapper.EventType.SLAVE_REPLAY);
        
        // 读取Cap'n Proto数据
        if (dataLength > 0) {
            log.debug("读取NexusWrapper数据内容 - dataLength: {}, node: {}", dataLength, nodeId);
            byte[] data = new byte[dataLength];
            int bytesRead = inputStream.read(data);
            if (bytesRead != dataLength) {
                log.warn("Incomplete data read - expected: {}, actual: {}, node: {}", dataLength, bytesRead, nodeId);
                return;
            }
            
            wrapper.getBuffer().writeBytes(data);
            log.debug("NexusWrapper数据读取完成 - id: {}, dataLength: {}, node: {}", id, dataLength, nodeId);
        } else {
            log.debug("NexusWrapper无数据内容 - id: {}, node: {}", id, nodeId);
        }
        
        // 发布到sequencer disruptor
        if (sequencerRingBuffer != null) {
            long sequence = sequencerRingBuffer.next();
            sequencerRingBuffer.publishEvent((eventWrapper, seq) -> {
                // 复制数据到新的wrapper
                eventWrapper.getBuffer().clear();
                eventWrapper.getBuffer().writeBytes(wrapper.getBuffer());
                eventWrapper.setId(wrapper.getId());
                eventWrapper.setPartition(wrapper.getPartition());
                eventWrapper.setEventType(wrapper.getEventType());
            });
            
            // 立即发送确认
            sendConfirmation(sequence);
            
            log.debug("NexusWrapper处理完成并发送确认 - id: {}, sequence: {}, node: {}", id, sequence, nodeId);
        } else {
            log.warn("SequencerRingBuffer未设置，无法处理NexusWrapper - node: {}", nodeId);
        }
    }
    
    /**
     * 处理批次消息
     */
    private void processBatchMessage() throws IOException {
        long batchId = inputStream.readLong();
        int eventCount = inputStream.readInt();
        long sequenceStart = inputStream.readLong();
        long sequenceEnd = inputStream.readLong();
        
        log.debug("处理批次消息 - batchId: {}, eventCount: {}, sequences: {}-{}, node: {}", 
                batchId, eventCount, sequenceStart, sequenceEnd, nodeId);
        
        // 处理批次中的每个事件
        for (int i = 0; i < eventCount; i++) {
            NexusWrapper wrapper = readNexusWrapper();
            if (wrapper != null) {
                // 发布到sequencer disruptor
                long sequence = sequencerRingBuffer.next();
                try {
                    sequencerRingBuffer.get(sequence).setEventType(NexusWrapper.EventType.SLAVE_REPLAY);
                    sequencerRingBuffer.publish(sequence);
                } catch (Exception e) {
                    log.error("Failed to publish event to sequencer disruptor", e);
                }
            }
        }
        
        // 等待批次结束标记
        int endMagic = inputStream.readInt();
        int endVersion = inputStream.readInt();
        int endMessageType = inputStream.readInt();
        
        if (endMagic != MAGIC_NUMBER || endVersion != VERSION || endMessageType != 2) {
            throw new IOException("Invalid batch end marker");
        }
        
        log.debug("批次处理完成 - batchId: {}, eventCount: {}, node: {}", batchId, eventCount, nodeId);
    }
    
    /**
     * 处理批次结束消息
     */
    private void processBatchEndMessage() throws IOException {
        log.debug("收到批次结束标记 - node: {}", nodeId);
    }
    
    /**
     * 处理心跳消息
     */
    private void processHeartbeatMessage() throws IOException {
        long timestamp = inputStream.readLong();
        log.debug("收到心跳消息 - timestamp: {}, node: {}", timestamp, nodeId);
        lastSuccessfulHeartbeat.set(System.currentTimeMillis());
    }
    
    /**
     * 读取NexusWrapper数据
     */
    private NexusWrapper readNexusWrapper() throws IOException {
        log.debug("开始读取NexusWrapper数据 - node: {}", nodeId);
        
        long id = inputStream.readLong();
        int combinedPartitionAndEventType = inputStream.readInt();
        int dataLength = inputStream.readInt();
        
        log.debug("读取NexusWrapper头信息 - id: {}, combinedPartitionAndEventType: {}, dataLength: {}, node: {}", 
                id, combinedPartitionAndEventType, dataLength, nodeId);
        
        // 创建NexusWrapper
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, dataLength);
        wrapper.setId(id);
        wrapper.setPartitionByCombined(combinedPartitionAndEventType);
        wrapper.setEventType(NexusWrapper.EventType.SLAVE_REPLAY);
        
        // 读取数据
        if (dataLength > 0) {
            log.debug("读取NexusWrapper数据内容 - dataLength: {}, node: {}", dataLength, nodeId);
            byte[] data = new byte[dataLength];
            int bytesRead = inputStream.read(data);
            if (bytesRead != dataLength) {
                log.warn("Incomplete data read - expected: {}, actual: {}, node: {}", dataLength, bytesRead, nodeId);
                return null;
            }
            
            wrapper.getBuffer().writeBytes(data);
            log.debug("NexusWrapper数据读取完成 - id: {}, dataLength: {}, node: {}", id, dataLength, nodeId);
        } else {
            log.debug("NexusWrapper无数据内容 - id: {}, node: {}", id, nodeId);
        }
        
        return wrapper;
    }
    
    /**
     * 发送确认消息（新协议，从节点模式）
     */
    public void sendConfirmation(long sequence) throws IOException {
        if (!isSlaveMode) {
            log.warn("sendConfirmation called in master mode - node: {}", nodeId);
            return;
        }
        
        if (!isConnected()) {
            log.warn("无法发送确认 - 客户端未连接, sequence: {}, node: {}", sequence, nodeId);
            return;
        }
        
        log.debug("发送新协议确认消息 - sequence: {}, node: {}", sequence, nodeId);
        
        // 使用新协议序列化确认消息
        ByteBuf confirmation = ReplicationProtocol.serializeConfirmation(sequence, nodeId);
        
        // 转换为字节数组发送
        byte[] data = new byte[confirmation.readableBytes()];
        confirmation.readBytes(data);
        outputStream.write(data);
        outputStream.flush();
        
        // 释放ByteBuf
        confirmation.release();
        
        log.debug("新协议确认消息发送成功 - sequence: {}, node: {}", sequence, nodeId);
    }
    
    /**
     * 发送确认消息（旧协议，从节点模式）
     */
    public void sendConfirmation(long batchId, long sequence) throws IOException {
        if (!isSlaveMode) {
            log.warn("sendConfirmation called in master mode - node: {}", nodeId);
            return;
        }
        
        if (!isConnected()) {
            log.warn("无法发送确认 - 客户端未连接, batchId: {}, sequence: {}, node: {}", batchId, sequence, nodeId);
            return;
        }
        
        log.debug("发送旧协议确认消息 - batchId: {}, sequence: {}, node: {}", batchId, sequence, nodeId);
        
        ByteBuffer confirmation = ByteBuffer.allocate(24);
        confirmation.putInt(MAGIC_NUMBER);
        confirmation.putInt(VERSION);
        confirmation.putInt(4); // Message Type: 4 = Confirmation
        confirmation.putLong(batchId);
        confirmation.putLong(sequence);
        
        outputStream.write(confirmation.array());
        outputStream.flush();
        
        log.debug("旧协议确认消息发送成功 - batchId: {}, sequence: {}, node: {}", batchId, sequence, nodeId);
    }
    
    /**
     * 发送批次到从节点
     */
    public void sendBatch(long batchId, List<NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        if (!isConnected()) {
            log.warn("无法发送批次 - 客户端未连接, batchId: {}, node: {}", batchId, nodeId);
            scheduleReconnect();
            throw new IllegalStateException("Client not connected");
        }
        
        try {
            log.info("开始发送批次到目标节点 - batchId: {}, eventCount: {}, sequences: {}-{}, node: {}", 
                    batchId, events.size(), sequenceStart, sequenceEnd, nodeId);
            
            // 发送批次头
            sendBatchHeader(batchId, events.size(), sequenceStart, sequenceEnd);
            
            // 发送每个NexusWrapper
            log.debug("开始发送批次中的 {} 个事件 - batchId: {}, node: {}", events.size(), batchId, nodeId);
            for (int i = 0; i < events.size(); i++) {
                NexusWrapper wrapper = events.get(i);
                log.debug("发送第 {}/{} 个事件 - batchId: {}, eventId: {}, node: {}", 
                        i+1, events.size(), batchId, wrapper.getId(), nodeId);
                sendNexusWrapper(wrapper);
            }
            
            // 发送批次结束标记
            sendBatchEnd();
            
            log.info("批次发送成功 - batchId: {}, eventCount: {}, node: {}", batchId, events.size(), nodeId);
            lastSuccessfulHeartbeat.set(System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("发送批次失败 - batchId: {}, eventCount: {}, node: {}", 
                    batchId, events.size(), nodeId, e);
            handleConnectionError(e);
            throw new RuntimeException("Failed to send batch", e);
        }
    }
    
    /**
     * 发送批次头
     */
    private void sendBatchHeader(long batchId, int eventCount, long sequenceStart, long sequenceEnd) throws IOException {
        log.debug("开始发送批次头 - batchId: {}, eventCount: {}, sequences: {}-{}, node: {}", 
                batchId, eventCount, sequenceStart, sequenceEnd, nodeId);
        
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
        
        log.debug("批次头发送成功 - batchId: {}, eventCount: {}, node: {}", batchId, eventCount, nodeId);
    }
    
    /**
     * 发送NexusWrapper数据
     */
    private void sendNexusWrapper(NexusWrapper wrapper) throws IOException {
        log.debug("开始发送NexusWrapper - id: {}, partition: {}, node: {}", 
                wrapper.getId(), wrapper.getPartition(), nodeId);
        
        ByteBuf buffer = wrapper.getBuffer();
        int readableBytes = buffer.readableBytes();
        
        log.debug("NexusWrapper数据大小 - id: {}, dataLength: {}, node: {}", 
                wrapper.getId(), readableBytes, nodeId);
        
        // 发送NexusWrapper头：ID (8 bytes) + Combined Partition&EventType (4 bytes) + Data Length (4 bytes) = 16 bytes
        ByteBuffer wrapperHeader = ByteBuffer.allocate(16);
        wrapperHeader.putLong(wrapper.getId());
        wrapperHeader.putInt(wrapper.getCombinedPartitionAndEventType());
        wrapperHeader.putInt(readableBytes);
        
        outputStream.write(wrapperHeader.array());
        
        // 发送实际数据
        if (readableBytes > 0) {
            byte[] data = new byte[readableBytes];
            buffer.getBytes(buffer.readerIndex(), data);
            outputStream.write(data);
            log.debug("NexusWrapper数据发送完成 - id: {}, dataLength: {}, node: {}", 
                    wrapper.getId(), readableBytes, nodeId);
        } else {
            log.debug("NexusWrapper无数据内容 - id: {}, node: {}", wrapper.getId(), nodeId);
        }
        
        outputStream.flush();
        log.debug("NexusWrapper发送成功 - id: {}, node: {}", wrapper.getId(), nodeId);
    }
    
    /**
     * 发送批次结束标记
     */
    private void sendBatchEnd() throws IOException {
        log.debug("开始发送批次结束标记 - node: {}", nodeId);
        
        ByteBuffer endMarker = ByteBuffer.allocate(12);
        endMarker.putInt(MAGIC_NUMBER);
        endMarker.putInt(VERSION);
        endMarker.putInt(2); // Message Type: 2 = Batch End
        
        outputStream.write(endMarker.array());
        outputStream.flush();
        
        log.debug("批次结束标记发送成功 - node: {}", nodeId);
    }
    
    /**
     * 发送心跳
     */
    public void sendHeartbeat() throws IOException {
        if (!isConnected()) {
            log.debug("无法发送心跳 - 连接未建立, node: {}", nodeId);
            return;
        }
        
        log.debug("开始发送心跳 - node: {}", nodeId);
        
        ByteBuffer heartbeat = ByteBuffer.allocate(16);
        heartbeat.putInt(MAGIC_NUMBER);
        heartbeat.putInt(VERSION);
        heartbeat.putInt(3); // Message Type: 3 = Heartbeat
        heartbeat.putLong(System.currentTimeMillis());
        
        outputStream.write(heartbeat.array());
        outputStream.flush();
        
        log.debug("心跳发送成功 - node: {}", nodeId);
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        log.info("Attempting to disconnect from target at {}:{} for node {}", host, port, nodeId);
        
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
                log.info("Successfully disconnected from target at {}:{} for node {}", host, port, nodeId);
            } catch (Exception e) {
                log.warn("Error closing socket", e);
            }
        } else {
            log.debug("Socket already closed or null for node {}", nodeId);
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
     * 获取节点ID
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * 检查是否为从节点模式
     */
    public boolean isSlaveMode() {
        return isSlaveMode;
    }
    
    /**
     * 获取sequencerRingBuffer（从节点模式）
     */
    public RingBuffer<NexusWrapper> getSequencerRingBuffer() {
        return sequencerRingBuffer;
    }
}
