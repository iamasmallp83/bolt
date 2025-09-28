package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationState;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
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
    private final ReplicationHandler replicationHandler;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, SlaveConnection> slaveConnections = new ConcurrentHashMap<>();
    
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    
    // TCP协议常量
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;
    
    public TcpReplicationServer(int port, BoltConfig config) {
        this.port = port;
        this.config = config;
        this.replicationState = new ReplicationState();
        this.replicationHandler = new ReplicationHandler(config, replicationState);
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
                            
                            // 添加长度字段解码器（4字节长度字段）
                            pipeline.addLast("lengthDecoder", new LengthFieldBasedFrameDecoder(
                                    Integer.MAX_VALUE, 0, 4, 0, 4));
                            pipeline.addLast("lengthEncoder", new LengthFieldPrepender(4));
                            
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
            replicationHandler.onShutdown();
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
            log.info("New slave connection from {}", slaveNodeId);
            
            // 创建从节点连接
            SlaveConnection connection = new SlaveConnection(slaveNodeId, ctx.channel());
            slaveConnections.put(slaveNodeId, connection);
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            try {
                if (msg instanceof io.netty.buffer.ByteBuf) {
                    io.netty.buffer.ByteBuf buffer = (io.netty.buffer.ByteBuf) msg;
                    processMessage(buffer);
                }
            } catch (Exception e) {
                log.error("Error processing message from slave {}", slaveNodeId, e);
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
         * 处理来自从节点的消息
         */
        private void processMessage(io.netty.buffer.ByteBuf buffer) {
            // 读取消息头
            int magic = buffer.readInt();
            if (magic != MAGIC_NUMBER) {
                throw new IllegalArgumentException("Invalid magic number: " + Integer.toHexString(magic));
            }
            
            int version = buffer.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported version: " + version);
            }
            
            int messageType = buffer.readInt();
            
            switch (messageType) {
                case 3: // Heartbeat
                    handleHeartbeat(buffer);
                    break;
                case 4: // Batch Ack
                    handleBatchAck(buffer);
                    break;
                default:
                    log.warn("Unknown message type: {}", messageType);
                    break;
            }
        }
        
        /**
         * 处理心跳
         */
        private void handleHeartbeat(io.netty.buffer.ByteBuf buffer) {
            long timestamp = buffer.readLong();
            log.debug("Received heartbeat from slave {} at {}", slaveNodeId, timestamp);
            
            // 更新心跳
            replicationHandler.handleSlaveHeartbeat(slaveNodeId);
            
            // 发送心跳响应
            io.netty.buffer.ByteBuf response = ctx.alloc().buffer(16);
            response.writeInt(MAGIC_NUMBER);
            response.writeInt(VERSION);
            response.writeInt(5); // Message Type: 5 = Heartbeat Response
            response.writeLong(System.currentTimeMillis());
            
            ctx.writeAndFlush(response);
        }
        
        /**
         * 处理批次确认
         */
        private void handleBatchAck(io.netty.buffer.ByteBuf buffer) {
            long batchId = buffer.readLong();
            long processedSequence = buffer.readLong();
            long timestamp = buffer.readLong();
            
            log.debug("Received batch ack from slave {}: batch={}, sequence={}", 
                    slaveNodeId, batchId, processedSequence);
            
            // 处理确认
            replicationHandler.handleBatchAcknowledgment(batchId, slaveNodeId);
            
            // 发送确认响应
            io.netty.buffer.ByteBuf response = ctx.alloc().buffer(20);
            response.writeInt(MAGIC_NUMBER);
            response.writeInt(VERSION);
            response.writeInt(6); // Message Type: 6 = Ack Response
            response.writeLong(batchId);
            response.writeLong(System.currentTimeMillis());
            
            ctx.writeAndFlush(response);
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
     * 获取复制处理器
     */
    public ReplicationHandler getReplicationHandler() {
        return replicationHandler;
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
}
