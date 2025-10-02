package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationProto;
import com.lmax.disruptor.RingBuffer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP复制客户端 - 从节点专用
 * 负责连接到主节点并接收复制请求
 */
@Slf4j
public class TcpReplicationClient {
    
    private final String masterHost;
    private final int masterPort;
    private final BoltConfig config;
    private final int nodeId;
    private final RingBuffer<NexusWrapper> sequencerRingBuffer;
    
    private EventLoopGroup workerGroup;
    private Channel clientChannel;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger heartbeatSequence = new AtomicInteger(0);
    private ScheduledExecutorService heartbeatExecutor;
    
    // TCP协议常量 - 现在使用 Protocol Buffers
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;
    private static final int HEARTBEAT_INTERVAL_SECONDS = 30;
    
    public TcpReplicationClient(String masterHost, int masterPort, BoltConfig config, 
                               RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.config = config;
        this.nodeId = config.nodeId();
        this.sequencerRingBuffer = sequencerRingBuffer;
    }
    
    /**
     * 连接到主节点
     */
    public void connect() throws InterruptedException {
        if (connected.compareAndSet(false, true)) {
            log.info("Connecting to master at {}:{}", masterHost, masterPort);
            
            workerGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("replication-client"));
            
            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast(new ReplicationClientHandler());
                            }
                        })
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .option(ChannelOption.TCP_NODELAY, true);
                
                ChannelFuture future = bootstrap.connect(masterHost, masterPort).sync();
                clientChannel = future.channel();
                
                log.info("Connected to master at {}:{}", masterHost, masterPort);
                
                // 连接成功后发送注册消息
                sendRegisterMessage();
                
                // 启动心跳任务
                startHeartbeat();
            } catch (Exception e) {
                log.error("Failed to connect to master at {}:{}", masterHost, masterPort, e);
                connected.set(false);
                throw e;
            }
        }
    }
    
    /**
     * 断开连接
     */
    public void disconnect() {
        if (connected.compareAndSet(true, false)) {
            log.info("Disconnecting from master");
            
            if (clientChannel != null && clientChannel.isActive()) {
                try {
                    clientChannel.close().sync();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while closing client channel", e);
                }
            }
            
            // 停止心跳任务
            stopHeartbeat();
            
            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            
            log.info("Disconnected from master");
        }
    }
    
    /**
     * 发送确认响应到主节点
     */
    public void sendConfirmation(long sequence, boolean success, String errorMessage) {
        if (!connected.get() || clientChannel == null || !clientChannel.isActive()) {
            log.warn("Not connected to master, cannot send confirmation - sequence: {}", sequence);
            return;
        }
        
        try {
            // 使用新的 Protocol Buffers 协议编码确认消息
            ByteBuf message = ReplicationProtocolUtils.encodeConfirmationMessage(sequence, nodeId, success, errorMessage);
            clientChannel.writeAndFlush(message);
            log.debug("Sent confirmation to master - sequence: {}, success: {}", sequence, success);
        } catch (Exception e) {
            log.error("Failed to send confirmation to master - sequence: {}", sequence, e);
        }
    }
    
    /**
     * 发送确认响应到主节点（简化版本）
     */
    public void sendConfirmation(long sequence) {
        sendConfirmation(sequence, true, null);
    }
    
    /**
     * 发送注册消息到主节点
     */
    private void sendRegisterMessage() {
        if (!connected.get() || clientChannel == null || !clientChannel.isActive()) {
            log.warn("Not connected to master, cannot send register message");
            return;
        }
        
        try {
            // 使用新的 Protocol Buffers 协议编码注册消息
            ByteBuf message = ReplicationProtocolUtils.encodeRegisterMessage(
                    nodeId, 
                    "localhost", // 从节点的host，这里简化处理
                    config.masterPort(), 
                    config.replicationPort(), 
                    ReplicationProto.NodeType.SLAVE
            );
            clientChannel.writeAndFlush(message);
            log.info("Sent register message to master - nodeId: {}", nodeId);
        } catch (Exception e) {
            log.error("Failed to send register message to master - nodeId: {}", nodeId, e);
        }
    }
    
    /**
     * 发送心跳消息到主节点
     */
    private void sendHeartbeatMessage() {
        if (!connected.get() || clientChannel == null || !clientChannel.isActive()) {
            log.warn("Not connected to master, cannot send heartbeat message");
            return;
        }
        
        try {
            int sequence = heartbeatSequence.incrementAndGet();
            // 使用新的 Protocol Buffers 协议编码心跳消息
            ByteBuf message = ReplicationProtocolUtils.encodeHeartbeatMessage(nodeId, sequence);
            clientChannel.writeAndFlush(message);
            log.debug("Sent heartbeat message to master - nodeId: {}, sequence: {}", nodeId, sequence);
        } catch (Exception e) {
            log.error("Failed to send heartbeat message to master - nodeId: {}", nodeId, e);
        }
    }
    
    /**
     * 启动心跳任务
     */
    private void startHeartbeat() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-sender-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        
        heartbeatExecutor.scheduleAtFixedRate(
                this::sendHeartbeatMessage,
                HEARTBEAT_INTERVAL_SECONDS,
                HEARTBEAT_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
        
        log.info("Started heartbeat task - interval: {} seconds", HEARTBEAT_INTERVAL_SECONDS);
    }
    
    /**
     * 停止心跳任务
     */
    private void stopHeartbeat() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                heartbeatExecutor.shutdownNow();
            }
            log.info("Stopped heartbeat task");
        }
    }
    
    /**
     * 获取节点ID
     */
    public int getNodeId() {
        return nodeId;
    }
    
    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return connected.get();
    }
    
    /**
     * 获取SequencerRingBuffer
     */
    public RingBuffer<NexusWrapper> getSequencerRingBuffer() {
        return sequencerRingBuffer;
    }
    
    /**
     * 复制客户端处理器
     */
    private class ReplicationClientHandler extends ChannelInboundHandlerAdapter {
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("Connected to master from slave: {}", nodeId);
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Disconnected from master, slave: {}", nodeId);
            connected.set(false);
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf buffer) {
                try {
                    // 使用新的 Protocol Buffers 协议解码消息
                    ReplicationProto.ReplicationMessage message = ReplicationProtocolUtils.decodeMessage(buffer);
                    
                    // 验证消息完整性
                    if (!ReplicationProtocolUtils.validateMessage(message)) {
                        log.error("Invalid message received from master");
                        ctx.close();
                        return;
                    }
                    
                    ReplicationProto.ProtocolHeader header = message.getHeader();
                    
                    // 根据消息类型处理
                    switch (header.getMessageType()) {
                        case REGISTER:
                            handleRegisterResponse(message.getRegisterResponse());
                            break;
                        case HEARTBEAT:
                            handleHeartbeatResponse(message.getHeartbeatResponse());
                            break;
                        case BUSINESS:
                            handleBusinessMessage(message.getBusiness());
                            break;
                        case CONFIRMATION:
                            // 从节点通常不会收到确认消息
                            log.warn("Received unexpected confirmation message from master");
                            break;
                        default:
                            log.warn("Unknown message type: {}", header.getMessageType());
                    }
                    
                } catch (Exception e) {
                    log.error("Error processing message from master", e);
                    ctx.close();
                }
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error in replication client handler", cause);
            ctx.close();
        }
        
        /**
         * 处理注册响应
         */
        private void handleRegisterResponse(ReplicationProto.RegisterResponse response) {
            log.info("Received register response - success: {}, message: {}, assignedNodeId: {}", 
                    response.getSuccess(), response.getMessage(), response.getAssignedNodeId());
            
            if (response.getSuccess()) {
                log.info("Successfully registered with master - assigned nodeId: {}", response.getAssignedNodeId());
            } else {
                log.error("Registration failed: {}", response.getMessage());
            }
        }
        
        /**
         * 处理心跳响应
         */
        private void handleHeartbeatResponse(ReplicationProto.HeartbeatResponse response) {
            log.debug("Received heartbeat response - success: {}, serverTime: {}, nextInterval: {}", 
                    response.getSuccess(), response.getServerTime(), response.getNextHeartbeatInterval());
        }
        
        /**
         * 处理业务消息
         */
        private void handleBusinessMessage(ReplicationProto.BusinessMessage businessMessage) {
            try {
                log.debug("Received business message - sequence: {}, partition: {}, eventType: {}", 
                        businessMessage.getSequence(), businessMessage.getPartition(), businessMessage.getEventType());
                
                // 发布事件到SequencerRingBuffer
                long sequence = sequencerRingBuffer.next();
                try {
                    NexusWrapper wrapper = sequencerRingBuffer.get(sequence);
                    
                    // 设置wrapper属性
                    wrapper.setId(businessMessage.getSequence());
                    wrapper.setPartition(businessMessage.getPartition());
                    wrapper.setEventType(NexusWrapper.EventType.fromValue(businessMessage.getEventType()));
                    byte[] data = businessMessage.getData().toByteArray();
                    wrapper.getBuffer().clear();
                    wrapper.getBuffer().writeBytes(data);
                    
                    // 确保buffer的readerIndex在正确位置
                    wrapper.getBuffer().readerIndex(0);
                    
                    // 添加调试信息
                    log.debug("Business message processed - sequence: {}, dataLength: {}, bufferReadableBytes: {}", 
                            businessMessage.getSequence(), data.length, wrapper.getBuffer().readableBytes());

                    // 发布事件
                    sequencerRingBuffer.publish(sequence);
                    
                    log.debug("Published business event to sequencer - sequence: {}", sequence);
                    
                } catch (Exception e) {
                    log.error("Failed to publish business event - sequence: {}", businessMessage.getSequence(), e);
                    // 发送失败确认
                    sendConfirmation(businessMessage.getSequence(), false, e.getMessage());
                }
                
            } catch (Exception e) {
                log.error("Failed to handle business message - sequence: {}", businessMessage.getSequence(), e);
                // 发送失败确认
                sendConfirmation(businessMessage.getSequence(), false, e.getMessage());
            }
        }
    }
}
