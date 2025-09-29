package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.RingBuffer;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP从节点客户端 - 连接到主节点并接收复制数据
 * 将接收到的NexusWrapper数据发送到sequencerDisruptor
 */
@Slf4j
public class TcpSlaveClient {
    
    private final String masterHost;
    private final int masterPort;
    private final String slaveNodeId;
    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
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
    
    public TcpSlaveClient(String masterHost, int masterPort, String slaveNodeId, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.slaveNodeId = slaveNodeId;
        this.sequencerRingBuffer = sequencerRingBuffer;
    }
    
    /**
     * 连接到主节点
     */
    public void connect() {
        log.info("Attempting to connect to master at {}:{} for slave {}", masterHost, masterPort, slaveNodeId);
        
        try {
            socket = new Socket(masterHost, masterPort);
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
            
            // 启动消息接收线程
            startMessageReceiver();
            
            log.info("Successfully connected to master at {}:{} for slave {}", masterHost, masterPort, slaveNodeId);
        } catch (Exception e) {
            log.error("Failed to connect to master at {}:{} for slave {}: {}", masterHost, masterPort, slaveNodeId, e.getMessage(), e);
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
     * 启动消息接收线程
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
     * 处理来自主节点的消息
     */
    private void processMessage() throws IOException {
        log.debug("开始读取来自主节点的消息 - slave: {}", slaveNodeId);
        
        // 读取消息头
        int magic = inputStream.readInt();
        log.debug("读取到magic number: {} (期望: {}) - slave: {}", 
                Integer.toHexString(magic), Integer.toHexString(MAGIC_NUMBER), slaveNodeId);
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }
        
        int version = inputStream.readInt();
        log.debug("读取到version: {} (期望: {}) - slave: {}", version, VERSION, slaveNodeId);
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        int messageType = inputStream.readInt();
        log.debug("读取到message type: {} - slave: {}", messageType, slaveNodeId);
        
        switch (messageType) {
            case 1: // Batch
                handleBatch();
                break;
            case 2: // Batch End
                handleBatchEnd();
                break;
            case 5: // Heartbeat Response
                handleHeartbeatResponse();
                break;
            case 6: // Ack Response
                handleAckResponse();
                break;
            default:
                log.warn("Unknown message type: {}", messageType);
                break;
        }
    }
    
    /**
     * 处理批次数据
     */
    private void handleBatch() throws IOException {
        log.debug("开始处理批次数据 - slave: {}", slaveNodeId);
        
        long batchId = inputStream.readLong();
        int eventCount = inputStream.readInt();
        long sequenceStart = inputStream.readLong();
        long sequenceEnd = inputStream.readLong();
        
        log.info("读取到批次数据 - batchId: {}, eventCount: {}, sequences: {}-{}, slave: {}", 
                batchId, eventCount, sequenceStart, sequenceEnd, slaveNodeId);
        
        // 处理批次中的每个NexusWrapper
        log.debug("开始处理批次中的 {} 个事件 - batchId: {}, slave: {}", eventCount, batchId, slaveNodeId);
        for (int i = 0; i < eventCount; i++) {
            log.debug("处理第 {}/{} 个事件 - batchId: {}, slave: {}", i+1, eventCount, batchId, slaveNodeId);
            NexusWrapper wrapper = readNexusWrapper();
            if (wrapper != null) {
                // 将NexusWrapper发送到sequencerDisruptor
                long sequence = sequencerRingBuffer.next();
                try {
                    sequencerRingBuffer.get(sequence).setId(wrapper.getId());
                    sequencerRingBuffer.get(sequence).setPartition(wrapper.getPartition());
                    
                    // 复制ByteBuf数据
                    ByteBuf sourceBuffer = wrapper.getBuffer();
                    ByteBuf targetBuffer = sequencerRingBuffer.get(sequence).getBuffer();
                    targetBuffer.clear();
                    targetBuffer.writeBytes(sourceBuffer, sourceBuffer.readableBytes());
                    
                    sequencerRingBuffer.publish(sequence);
                    
                    log.debug("成功发布NexusWrapper到sequencerDisruptor - id: {}, sequence: {}, batchId: {}, slave: {}", 
                            wrapper.getId(), sequence, batchId, slaveNodeId);
                } catch (Exception e) {
                    log.error("发布NexusWrapper到sequencerDisruptor失败 - id: {}, sequence: {}, batchId: {}, slave: {}", 
                            wrapper.getId(), sequence, batchId, slaveNodeId, e);
                    sequencerRingBuffer.publish(sequence); // 确保序列号被释放
                }
            } else {
                log.warn("读取到空的NexusWrapper - 第 {}/{} 个事件, batchId: {}, slave: {}", 
                        i+1, eventCount, batchId, slaveNodeId);
            }
        }
        
        // 发送批次确认
        log.debug("发送批次确认 - batchId: {}, sequenceEnd: {}, slave: {}", batchId, sequenceEnd, slaveNodeId);
        sendBatchAcknowledgment(batchId, sequenceEnd);
        log.info("批次处理完成 - batchId: {}, eventCount: {}, slave: {}", batchId, eventCount, slaveNodeId);
    }
    
    /**
     * 读取NexusWrapper数据
     */
    private NexusWrapper readNexusWrapper() throws IOException {
        log.debug("开始读取NexusWrapper数据 - slave: {}", slaveNodeId);
        
        long id = inputStream.readLong();
        int partition = inputStream.readInt();
        int dataLength = inputStream.readInt();
        
        log.debug("读取NexusWrapper头信息 - id: {}, partition: {}, dataLength: {}, slave: {}", 
                id, partition, dataLength, slaveNodeId);
        
        // 创建NexusWrapper
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, dataLength);
        wrapper.setId(id);
        wrapper.setPartition(partition);
        
        // 读取数据
        if (dataLength > 0) {
            log.debug("读取NexusWrapper数据内容 - dataLength: {}, slave: {}", dataLength, slaveNodeId);
            byte[] data = new byte[dataLength];
            inputStream.readFully(data);
            wrapper.getBuffer().writeBytes(data);
            log.debug("成功读取NexusWrapper数据 - id: {}, dataLength: {}, slave: {}", id, dataLength, slaveNodeId);
        } else {
            log.debug("NexusWrapper无数据内容 - id: {}, slave: {}", id, slaveNodeId);
        }
        
        return wrapper;
    }
    
    /**
     * 处理批次结束
     */
    private void handleBatchEnd() {
        log.debug("Received batch end marker");
    }
    
    /**
     * 处理心跳响应
     */
    private void handleHeartbeatResponse() throws IOException {
        long masterTimestamp = inputStream.readLong();
        log.debug("Received heartbeat response from master at {}", masterTimestamp);
        lastSuccessfulHeartbeat.set(System.currentTimeMillis());
    }
    
    /**
     * 处理确认响应
     */
    private void handleAckResponse() throws IOException {
        long batchId = inputStream.readLong();
        long masterTimestamp = inputStream.readLong();
        log.debug("Received ack response for batch {} from master at {}", batchId, masterTimestamp);
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
        
        ByteBuffer heartbeat = ByteBuffer.allocate(20);
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
            log.warn("无法发送批次确认 - 客户端未连接, batchId: {}, slave: {}", batchId, slaveNodeId);
            return;
        }
        
        log.debug("开始发送批次确认 - batchId: {}, processedSequence: {}, slave: {}", 
                batchId, processedSequence, slaveNodeId);
        
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
            
            log.info("批次确认发送成功 - batchId: {}, processedSequence: {}, slave: {}", 
                    batchId, processedSequence, slaveNodeId);
        } catch (Exception e) {
            log.error("发送批次确认失败 - batchId: {}, processedSequence: {}, slave: {}", 
                    batchId, processedSequence, slaveNodeId, e);
            handleConnectionError(e);
        }
    }
    
    /**
     * 发送确认消息给主节点（用于屏障机制）
     */
    public void sendConfirmation(long batchId, long sequence) throws IOException {
        if (!isConnected()) {
            log.warn("Cannot send confirmation - client not connected");
            return;
        }
        
        try {
            ByteBuffer confirmation = ByteBuffer.allocate(28);
            confirmation.putInt(MAGIC_NUMBER);
            confirmation.putInt(VERSION);
            confirmation.putInt(7); // Message Type: 7 = Confirmation
            confirmation.putLong(batchId);
            confirmation.putLong(sequence);
            confirmation.putLong(System.currentTimeMillis());
            
            outputStream.write(confirmation.array());
            outputStream.flush();
            
            log.debug("Confirmation sent to master for batch {} sequence {}", batchId, sequence);
        } catch (IOException e) {
            log.error("Failed to send confirmation to master for batch {} sequence {}", batchId, sequence, e);
            throw e;
        }
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        log.info("Attempting to disconnect from master at {}:{} for slave {}", masterHost, masterPort, slaveNodeId);
        
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
                log.info("Successfully disconnected from master at {}:{} for slave {}", masterHost, masterPort, slaveNodeId);
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
