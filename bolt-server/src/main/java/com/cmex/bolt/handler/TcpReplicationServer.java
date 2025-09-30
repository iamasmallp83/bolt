package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP复制服务器 - 使用Netty实现
 * 负责接收从节点的连接并处理复制数据
 */
@Slf4j
public class TcpReplicationServer {
    
    private final int port;
    private final BoltConfig config;
    private final ReplicationState replicationState;
    private final ReplicationHandler replicationHandler; // ReplicationHandler用于发送数据和确认管理
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, SlaveConnection> slaveConnections = new ConcurrentHashMap<>();
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    
    // TCP协议常量
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;
    
    public TcpReplicationServer(int port, BoltConfig config, ReplicationHandler replicationHandler) {
        this.port = port;
        this.config = config;
        this.replicationState = new ReplicationState();
        this.replicationHandler = replicationHandler;
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "tcp-replication-server-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * 启动TCP服务器
     */
    public void start() throws InterruptedException {
        log.info("Starting TCP replication server on port {}", port);
        
        running.set(true);
        
        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("tcp-replication-boss", true));
        workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("tcp-replication-worker", true));
        
        log.info("Created Netty event loop groups for TCP replication server");
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            
                            // 移除长度字段解码器，直接处理原始TCP数据
                            // 从节点发送的是原始TCP数据，不需要长度字段
                            
                            // 添加业务处理器
                            pipeline.addLast("replicationHandler", new ReplicationChannelHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);
            
            log.info("Binding TCP replication server to port {}", port);
            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            
            log.info("TCP replication server successfully started and listening on port {}", port);
            
        } catch (Exception e) {
            log.error("Failed to start TCP replication server on port {}", port, e);
            throw e;
        }
    }
    
    /**
     * 停止TCP服务器
     */
    public void stop() {
        log.info("Stopping TCP replication server");
        running.set(false);
        
        // 关闭所有从节点连接
        for (SlaveConnection connection : slaveConnections.values()) {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("Error closing slave connection", e);
            }
        }
        slaveConnections.clear();
        
        // 关闭复制处理器
        try {
            if (replicationHandler != null) {
                replicationHandler.onShutdown();
            }
        } catch (Exception e) {
            log.warn("Error shutting down replication handler", e);
        }
        
        // 关闭Netty服务器
        if (serverChannel != null) {
            serverChannel.close();
        }
        
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        
        // 关闭线程池
        executorService.shutdown();
        
        log.info("TCP replication server stopped");
    }
    
    /**
     * 检查服务器是否运行
     */
    public boolean isRunning() {
        return running.get() && serverChannel != null && serverChannel.isActive();
    }
    
    /**
     * 等待服务器关闭
     */
    public void awaitTermination() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }
    
    /**
     * Netty通道处理器
     */
    private class ReplicationChannelHandler extends ChannelInboundHandlerAdapter {
        private String slaveNodeId;
        private ChannelHandlerContext ctx;
        
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            slaveNodeId = ctx.channel().remoteAddress().toString();
            log.info("新的从节点连接 - slave: {}, 远程地址: {}", slaveNodeId, ctx.channel().remoteAddress());
            
            // 创建从节点连接
            SlaveConnection connection = new SlaveConnection(slaveNodeId, ctx.channel());
            slaveConnections.put(slaveNodeId, connection);
            log.info("从节点连接已建立 - slave: {}, 当前活跃连接数: {}", slaveNodeId, slaveConnections.size());
            
            // 注册从节点到ReplicationHandler
            if (replicationHandler != null) {
                // 使用配置的replicationPort而不是随机端口
                String[] parts = slaveNodeId.replace("/", "").split(":");
                if (parts.length >= 2) {
                    String host = parts[0];
                    // 使用slave配置的replicationPort，而不是随机端口
                    int port = config.replicationPort();
                    replicationHandler.registerSlave(slaveNodeId, host, port);
                    log.info("从节点已注册到ReplicationHandler - slave: {} at {}:{}", slaveNodeId, host, port);
                } else {
                    log.warn("无法解析从节点地址: {}", slaveNodeId);
                }
            }
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof io.netty.buffer.ByteBuf) {
                    io.netty.buffer.ByteBuf buffer = (io.netty.buffer.ByteBuf) msg;
                    log.debug("接收到来自从节点的消息 - slave: {}, 消息大小: {} bytes", slaveNodeId, buffer.readableBytes());
                    
                    // 处理消息，可能需要处理多个消息或部分消息
                    processRawMessage(buffer);
                    
                    log.debug("消息处理完成 - slave: {}", slaveNodeId);
                }
            } catch (Exception e) {
                log.error("处理来自从节点的消息时发生错误 - slave: {}", slaveNodeId, e);
            } finally {
                if (msg instanceof io.netty.buffer.ByteBuf) {
                    ((io.netty.buffer.ByteBuf) msg).release();
                }
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Slave connection closed: {}", slaveNodeId);
            if (slaveNodeId != null) {
                slaveConnections.remove(slaveNodeId);
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Exception in replication channel for slave {}", slaveNodeId, cause);
            ctx.close();
        }
        
        /**
         * 处理原始TCP消息
         */
        private void processRawMessage(io.netty.buffer.ByteBuf buffer) {
            log.debug("开始处理原始TCP消息 - slave: {}, 可读字节数: {}", slaveNodeId, buffer.readableBytes());
            
            // 循环处理缓冲区中的所有完整消息
            while (buffer.readableBytes() >= 12) { // 最小消息头大小：Magic(4) + Version(4) + MessageType(4)
                // 标记读取位置，以便在消息不完整时回退
                buffer.markReaderIndex();
                
                try {
                    // 读取消息头
                    int magic = buffer.readInt();
                    log.debug("读取到magic number: {} (期望: {}) - slave: {}", 
                            Integer.toHexString(magic), Integer.toHexString(MAGIC_NUMBER), slaveNodeId);
                    
                    if (magic != MAGIC_NUMBER) {
                        log.error("无效的magic number: {} - slave: {}", Integer.toHexString(magic), slaveNodeId);
                        break; // 停止处理，避免解析错误
                    }
                    
                    int version = buffer.readInt();
                    log.debug("读取到version: {} (期望: {}) - slave: {}", version, VERSION, slaveNodeId);
                    
                    if (version != VERSION) {
                        log.error("不支持的版本: {} - slave: {}", version, slaveNodeId);
                        break;
                    }
                    
                    int messageType = buffer.readInt();
                    log.debug("读取到message type: {} - slave: {}", messageType, slaveNodeId);
                    
                    // 根据消息类型处理
                    boolean processed = processMessageByType(buffer, messageType);
                    if (!processed) {
                        log.warn("消息处理失败，停止处理后续消息 - slave: {}, messageType: {}", slaveNodeId, messageType);
                        break;
                    }
                    
                } catch (Exception e) {
                    log.error("处理消息时发生错误 - slave: {}, 回退到标记位置", slaveNodeId, e);
                    buffer.resetReaderIndex(); // 回退到标记位置
                    break;
                }
            }
            
            log.debug("原始TCP消息处理完成 - slave: {}, 剩余字节数: {}", slaveNodeId, buffer.readableBytes());
        }
        
        /**
         * 根据消息类型处理消息
         */
        private boolean processMessageByType(io.netty.buffer.ByteBuf buffer, int messageType) {
            try {
                switch (messageType) {
                    case 2: // Confirmation (新协议)
                        return handleNewConfirmation(buffer);
                    case 3: // Heartbeat
                        return handleHeartbeat(buffer);
                    case 4: // Batch Ack (旧协议)
                        return handleBatchAck(buffer);
                    case 7: // Confirmation (旧协议)
                        return handleConfirmation(buffer);
                    default:
                        log.warn("未知的消息类型: {} - slave: {}", messageType, slaveNodeId);
                        return false;
                }
            } catch (Exception e) {
                log.error("处理消息类型 {} 时发生错误 - slave: {}", messageType, slaveNodeId, e);
                return false;
            }
        }
        
        /**
         * 处理新的确认消息（新协议）
         */
        private boolean handleNewConfirmation(io.netty.buffer.ByteBuf buffer) {
            try {
                // 转换为grpc ByteBuf
                io.grpc.netty.shaded.io.netty.buffer.ByteBuf grpcBuffer = 
                    io.grpc.netty.shaded.io.netty.buffer.Unpooled.wrappedBuffer(buffer.nioBuffer());
                
                ReplicationProtocol.ConfirmationMessage confirmation = ReplicationProtocol.deserializeConfirmation(grpcBuffer);
                log.debug("接收到新协议确认消息 - slave: {}, sequence: {}", confirmation.getSlaveNodeId(), confirmation.getSequence());
                
                // 直接通知ReplicationHandler
                if (replicationHandler != null) {
                    replicationHandler.handleSlaveConfirmation(confirmation.getSequence(), confirmation.getSlaveNodeId());
                }
                
                return true;
            } catch (Exception e) {
                log.error("处理新协议确认消息失败 - slave: {}", slaveNodeId, e);
                return false;
            }
        }
        
        /**
         * 处理心跳
         */
        private boolean handleHeartbeat(io.netty.buffer.ByteBuf buffer) {
            // 心跳消息：Magic(4) + Version(4) + MessageType(4) + Timestamp(8) = 20字节
            if (buffer.readableBytes() < 8) {
                log.warn("心跳消息不完整 - slave: {}, 需要8字节，实际: {}字节", slaveNodeId, buffer.readableBytes());
                return false;
            }
            
            long timestamp = buffer.readLong();
            log.debug("接收到心跳 - slave: {}, timestamp: {}", slaveNodeId, timestamp);
            
            // 更新ReplicationState中SlaveNode的心跳时间
            if (replicationHandler != null) {
                replicationHandler.handleSlaveHeartbeat(slaveNodeId);
                log.debug("已更新slave心跳时间 - slave: {}", slaveNodeId);
            }
            
            // 发送心跳响应
            log.debug("开始发送心跳响应 - slave: {}", slaveNodeId);
            io.netty.buffer.ByteBuf response = ctx.alloc().buffer(16);
            response.writeInt(MAGIC_NUMBER);
            response.writeInt(VERSION);
            response.writeInt(5); // Message Type: 5 = Heartbeat Response
            response.writeLong(System.currentTimeMillis());
            
            ctx.writeAndFlush(response);
            log.debug("心跳响应发送成功 - slave: {}", slaveNodeId);
            
            return true;
        }
        
        /**
         * 处理批次确认
         */
        private boolean handleBatchAck(io.netty.buffer.ByteBuf buffer) {
            // 批次确认消息：Magic(4) + Version(4) + MessageType(4) + BatchId(8) + ProcessedSequence(8) + Timestamp(8) = 36字节
            if (buffer.readableBytes() < 24) {
                log.warn("批次确认消息不完整 - slave: {}, 需要24字节，实际: {}字节", slaveNodeId, buffer.readableBytes());
                return false;
            }
            
            long batchId = buffer.readLong();
            long processedSequence = buffer.readLong();
            long timestamp = buffer.readLong();
            
            log.info("接收到批次确认 - slave: {}, batchId: {}, processedSequence: {}, timestamp: {}", 
                    slaveNodeId, batchId, processedSequence, timestamp);
            
            // 发送确认响应
            log.debug("开始发送批次确认响应 - slave: {}, batchId: {}", slaveNodeId, batchId);
            io.netty.buffer.ByteBuf response = ctx.alloc().buffer(20);
            response.writeInt(MAGIC_NUMBER);
            response.writeInt(VERSION);
            response.writeInt(6); // Message Type: 6 = Ack Response
            response.writeLong(batchId);
            response.writeLong(System.currentTimeMillis());
            
            ctx.writeAndFlush(response);
            log.debug("批次确认响应发送成功 - slave: {}, batchId: {}", slaveNodeId, batchId);
            
            return true;
        }
        
        /**
         * 处理确认消息（用于屏障机制）
         */
        private boolean handleConfirmation(io.netty.buffer.ByteBuf buffer) {
            // 确认消息：Magic(4) + Version(4) + MessageType(4) + BatchId(8) + Sequence(8) + Timestamp(8) = 36字节
            if (buffer.readableBytes() < 24) {
                log.warn("确认消息不完整 - slave: {}, 需要24字节，实际: {}字节", slaveNodeId, buffer.readableBytes());
                return false;
            }
            
            long batchId = buffer.readLong();
            long sequence = buffer.readLong();
            long timestamp = buffer.readLong();
            
            log.info("接收到确认消息 - slave: {}, batchId: {}, sequence: {}, timestamp: {}", 
                    slaveNodeId, batchId, sequence, timestamp);
            
            // 处理确认（需要传递给ReplicationHandler）
            if (replicationHandler != null) {
                log.debug("将确认消息传递给ReplicationHandler - slave: {}, sequence: {}", 
                        slaveNodeId, sequence);
                replicationHandler.handleSlaveConfirmation(sequence, slaveNodeId);
            } else {
                log.warn("ReplicationHandler为空，无法处理确认消息 - slave: {}", slaveNodeId);
            }
            
            // 发送确认响应
            log.debug("开始发送确认响应 - slave: {}, batchId: {}", slaveNodeId, batchId);
            io.netty.buffer.ByteBuf response = ctx.alloc().buffer(21);
            response.writeInt(MAGIC_NUMBER);
            response.writeInt(VERSION);
            response.writeInt(8); // Message Type: 8 = Confirmation Response
            response.writeLong(batchId);
            response.writeLong(System.currentTimeMillis());
            
            ctx.writeAndFlush(response);
            log.debug("确认响应发送成功 - slave: {}, batchId: {}", slaveNodeId, batchId);
            
            return true;
        }
    }
    
    /**
     * 从节点连接信息
     */
    private static class SlaveConnection {
        private final String nodeId;
        private final Channel channel;
        private volatile long lastHeartbeat;
        
        public SlaveConnection(String nodeId, Channel channel) {
            this.nodeId = nodeId;
            this.channel = channel;
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public boolean isHealthy(long timeoutMs) {
            return channel.isActive() && (System.currentTimeMillis() - lastHeartbeat) < timeoutMs;
        }
        
        public void close() {
            if (channel != null && channel.isActive()) {
                channel.close();
            }
        }
        
        // Getters
        public String getNodeId() { return nodeId; }
        public Channel getChannel() { return channel; }
        public long getLastHeartbeat() { return lastHeartbeat; }
    }
    
    /**
     * 获取活跃从节点数量
     */
    public int getActiveSlaveCount() {
        return slaveConnections.size();
    }
    
    /**
     * 获取所有活跃的从节点ID
     */
    public java.util.Set<String> getActiveSlaveIds() {
        return new java.util.HashSet<>(slaveConnections.keySet());
    }
    
    /**
     * 发送单个NexusWrapper到指定的从节点（新协议）
     */
    public void sendNexusWrapperToSlave(String slaveNodeId, com.cmex.bolt.core.NexusWrapper wrapper) {
        SlaveConnection connection = slaveConnections.get(slaveNodeId);
        if (connection != null && connection.getChannel().isActive()) {
            try {
                log.debug("发送NexusWrapper到slave - id: {}, slave: {}", wrapper.getId(), slaveNodeId);
                
                // 使用新的协议序列化NexusWrapper
                io.grpc.netty.shaded.io.netty.buffer.ByteBuf grpcMessage = ReplicationProtocol.serializeNexusWrapper(wrapper);
                
                // 转换为netty ByteBuf
                io.netty.buffer.ByteBuf message = io.netty.buffer.Unpooled.wrappedBuffer(grpcMessage.nioBuffer());
                connection.getChannel().writeAndFlush(message);
                
                // 释放grpc ByteBuf
                grpcMessage.release();
                
                log.debug("成功发送NexusWrapper到slave - id: {}, slave: {}", wrapper.getId(), slaveNodeId);
            } catch (Exception e) {
                log.error("发送NexusWrapper到slave失败 - id: {}, slave: {}", wrapper.getId(), slaveNodeId, e);
            }
        } else {
            log.warn("无法发送NexusWrapper到slave - 连接不存在或未激活 - slave: {}", slaveNodeId);
        }
    }
    
    /**
     * 发送批次到指定的从节点（保留旧协议用于兼容性）
     */
    public void sendBatchToSlave(String slaveNodeId, long batchId, java.util.List<com.cmex.bolt.core.NexusWrapper> events, long sequenceStart, long sequenceEnd) {
        SlaveConnection connection = slaveConnections.get(slaveNodeId);
        if (connection != null && connection.getChannel().isActive()) {
            try {
                log.debug("通过现有连接发送批次到slave - batchId: {}, eventCount: {}, slave: {}", 
                        batchId, events.size(), slaveNodeId);
                
                // 发送批次头
                io.netty.buffer.ByteBuf header = connection.getChannel().alloc().buffer(40);
                header.writeInt(MAGIC_NUMBER);
                header.writeInt(VERSION);
                header.writeInt(1); // Message Type: 1 = Batch
                header.writeLong(batchId);
                header.writeInt(events.size());
                header.writeLong(sequenceStart);
                header.writeLong(sequenceEnd);
                
                connection.getChannel().writeAndFlush(header);
                
                // 发送每个NexusWrapper
                for (com.cmex.bolt.core.NexusWrapper wrapper : events) {
                    io.netty.buffer.ByteBuf wrapperBuf = connection.getChannel().alloc().buffer(16 + wrapper.getBuffer().readableBytes());
                    wrapperBuf.writeLong(wrapper.getId());
                    wrapperBuf.writeInt(wrapper.getCombinedPartitionAndEventType());
                    wrapperBuf.writeInt(wrapper.getBuffer().readableBytes());
                    
                    // 复制ByteBuf数据
                    byte[] data = new byte[wrapper.getBuffer().readableBytes()];
                    wrapper.getBuffer().getBytes(wrapper.getBuffer().readerIndex(), data);
                    wrapperBuf.writeBytes(data);
                    
                    connection.getChannel().writeAndFlush(wrapperBuf);
                }
                
                // 发送批次结束标记
                io.netty.buffer.ByteBuf endMarker = connection.getChannel().alloc().buffer(12);
                endMarker.writeInt(MAGIC_NUMBER);
                endMarker.writeInt(VERSION);
                endMarker.writeInt(2); // Message Type: 2 = Batch End
                
                connection.getChannel().writeAndFlush(endMarker);
                
                log.info("成功通过现有连接发送批次到slave - batchId: {}, eventCount: {}, slave: {}", 
                        batchId, events.size(), slaveNodeId);
                        
            } catch (Exception e) {
                log.error("通过现有连接发送批次失败 - batchId: {}, slave: {}", batchId, slaveNodeId, e);
            }
        } else {
            log.warn("Slave连接不存在或已断开 - slave: {}", slaveNodeId);
        }
    }
}
