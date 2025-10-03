package com.cmex.bolt.handler;

import com.cmex.bolt.replication.ReplicationProto;
import com.cmex.bolt.replication.ReplicationState;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP复制服务器 - 主节点专用
 * 负责接收从节点连接并发送复制请求
 */
@Slf4j
public class TcpReplicationServer {

    private final int port;
    private final ReplicationState replicationState;
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, SlaveConnection> slaveConnections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> connectionToNodeId = new ConcurrentHashMap<>();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // TCP协议常量 - 现在使用 Protocol Buffers
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;

    public TcpReplicationServer(int port, ReplicationState replicationState) {
        this.port = port;
        this.replicationState = replicationState;
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "tcp-replication-server-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动服务器
     */
    public void start() throws InterruptedException {
        if (running.compareAndSet(false, true)) {
            log.info("Starting TcpReplicationServer on port {}", port);

            bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("replication-boss"));
            workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                    new DefaultThreadFactory("replication-worker"));

            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast(new ReplicationServerHandler());
                            }
                        })
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);

                ChannelFuture future = bootstrap.bind(port).sync();
                serverChannel = future.channel();

                log.info("TcpReplicationServer started successfully on port {}", port);
            } catch (Exception e) {
                log.error("Failed to start TcpReplicationServer on port {}", port, e);
                running.set(false);
                throw e;
            }
        }
    }

    /**
     * 停止服务器
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping TcpReplicationServer");

            // 关闭所有从节点连接
            for (SlaveConnection connection : slaveConnections.values()) {
                try {
                    connection.close();
                } catch (Exception e) {
                    log.warn("Error closing slave connection", e);
                }
            }
            slaveConnections.clear();

            // 关闭Netty服务器
            if (serverChannel != null) {
                try {
                    serverChannel.close().sync();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while closing server channel", e);
                }
            }

            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
            }
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
            }

            if (executorService != null) {
                executorService.shutdown();
            }

            log.info("TcpReplicationServer stopped");
        }
    }


    /**
     * 发送批处理复制请求到指定从节点
     */
    public void sendBatchReplicationRequest(int nodeId, ByteBuf batchMessage) {
        // 通过nodeId找到对应的连接地址
        String slaveNodeId = null;
        for (java.util.Map.Entry<String, Integer> entry : connectionToNodeId.entrySet()) {
            if (entry.getValue().equals(nodeId)) {
                slaveNodeId = entry.getKey();
                break;
            }
        }
        
        if (slaveNodeId == null) {
            log.warn("No connection found for nodeId: {}", nodeId);
            return;
        }
        
        SlaveConnection connection = slaveConnections.get(slaveNodeId);
        if (connection == null) {
            log.warn("No connection found for slave: {}", slaveNodeId);
            return;
        }

        try {
            // 发送批处理复制请求
            connection.sendBatchReplicationRequest(batchMessage);
            log.debug("Sent batch replication request to slave {} (nodeId: {})", slaveNodeId, nodeId);
        } catch (Exception e) {
            log.error("Failed to send batch replication request to slave {} (nodeId: {})", slaveNodeId, nodeId, e);
            replicationState.setSlaveConnected(nodeId, false);
        }
    }

    /**
     * 处理注册消息
     */
    private void handleRegisterMessage(ReplicationProto.RegisterMessage registerMessage, String slaveNodeId) {
        log.info("Received register message from slave: {} - nodeId: {}, host: {}, port: {}", 
                slaveNodeId, registerMessage.getNodeId(), registerMessage.getHost(), registerMessage.getPort());
        
        // 建立连接地址和nodeId的映射
        connectionToNodeId.put(slaveNodeId, registerMessage.getNodeId());
        
        // 注册从节点到ReplicationState
        replicationState.registerSlave(registerMessage.getNodeId(), registerMessage.getHost(), registerMessage.getPort());
        replicationState.setSlaveConnected(registerMessage.getNodeId(), true);
        
        // 发送注册响应
        sendRegisterResponse(slaveNodeId, registerMessage.getNodeId(), true, "Registration successful");
    }
    
    /**
     * 处理心跳消息
     */
    private void handleHeartbeatMessage(ReplicationProto.HeartbeatMessage heartbeatMessage, String slaveNodeId) {
        log.debug("Received heartbeat from slave: {} - nodeId: {}, sequence: {}", 
                slaveNodeId, heartbeatMessage.getNodeId(), heartbeatMessage.getSequence());
        
        // 更新从节点状态
        replicationState.setSlaveConnected(heartbeatMessage.getNodeId(), true);
        
        // 发送心跳响应
        sendHeartbeatResponse(slaveNodeId, heartbeatMessage.getSequence());
    }
    
    /**
     * 处理业务消息（从节点发送给主节点的业务数据）
     */
    private void handleBusinessMessage(ReplicationProto.BusinessMessage businessMessage, String slaveNodeId) {
        log.debug("Received business message from slave: {} - sequence: {}, partition: {}", 
                slaveNodeId, businessMessage.getSequence(), businessMessage.getPartition());
        
        // 这里可以处理从节点发送的业务数据
        // 通常主节点不需要处理从节点的业务数据，但可以用于同步检查
    }
    
    /**
     * 处理确认消息
     */
    private void handleConfirmationMessage(ReplicationProto.ConfirmationMessage confirmationMessage, String slaveNodeId) {
        log.debug("Received confirmation from slave: {} - sequence: {}, success: {}", 
                slaveNodeId, confirmationMessage.getSequence(), confirmationMessage.getSuccess());
        
        // ConfirmHandler已移除，不再需要处理确认
    }

    /**
     * 处理批处理业务消息（从节点发送给主节点）
     */
    private void handleBatchBusinessMessage(ReplicationProto.BatchBusinessMessage batchMessage, String slaveNodeId) {
        log.debug("Received batch business message from slave {} - batchSize: {}, startSequence: {}, endSequence: {}", 
                slaveNodeId, batchMessage.getBatchSize(), batchMessage.getStartSequence(), batchMessage.getEndSequence());
        
        // 主节点通常不会收到从节点的批处理业务消息
        // 这里可以添加相应的处理逻辑，比如日志记录或错误处理
        log.warn("Unexpected batch business message received from slave {} - this should not happen", slaveNodeId);
    }
    
    /**
     * 发送注册响应
     */
    private void sendRegisterResponse(String slaveNodeId, int assignedNodeId, boolean success, String message) {
        // 这里可以实现发送注册响应的逻辑
        log.info("Sending register response to slave: {} - success: {}, message: {}", slaveNodeId, success, message);
    }
    
    /**
     * 发送心跳响应
     */
    private void sendHeartbeatResponse(String slaveNodeId, int sequence) {
        // 这里可以实现发送心跳响应的逻辑
        log.debug("Sending heartbeat response to slave: {} - sequence: {}", slaveNodeId, sequence);
    }

    /**
     * 从节点连接处理器
     */
    private class ReplicationServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            String slaveNodeId = ctx.channel().remoteAddress().toString();
            SlaveConnection connection = new SlaveConnection(ctx.channel(), slaveNodeId);
            slaveConnections.put(slaveNodeId, connection);

            log.info("Slave connected: {}", slaveNodeId);
            
            // 注册从节点到ReplicationState
            // 注意：这里暂时不注册，等待收到注册消息后再注册
            // 因为我们需要从注册消息中获取正确的nodeId

            // ConfirmHandler已移除，不再需要注册
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            String slaveNodeId = ctx.channel().remoteAddress().toString();
            slaveConnections.remove(slaveNodeId);
            
            // 获取对应的nodeId并清理
            Integer nodeId = connectionToNodeId.remove(slaveNodeId);
            if (nodeId != null) {
                replicationState.setSlaveConnected(nodeId, false);
                replicationState.unregisterSlave(nodeId);
            }

            log.info("Slave disconnected: {}", slaveNodeId);

            // ConfirmHandler已移除，不再需要移除
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf buffer) {
                try {
                    // 使用新的 Protocol Buffers 协议解码消息
                    ReplicationProto.ReplicationMessage message = ReplicationProtocolUtils.decodeMessage(buffer);
                    
                    // 验证消息完整性
                    if (!ReplicationProtocolUtils.validateMessage(message)) {
                        log.error("Invalid message received from slave");
                        ctx.close();
                        return;
                    }
                    
                    ReplicationProto.ProtocolHeader header = message.getHeader();
                    String slaveNodeId = ctx.channel().remoteAddress().toString();
                    
                    // 根据消息类型处理
                    switch (header.getMessageType()) {
                        case REGISTER:
                            handleRegisterMessage(message.getRegister(), slaveNodeId);
                            break;
                        case HEARTBEAT:
                            handleHeartbeatMessage(message.getHeartbeat(), slaveNodeId);
                            break;
                        case BUSINESS:
                            handleBusinessMessage(message.getBusiness(), slaveNodeId);
                            break;
                        case CONFIRMATION:
                            handleConfirmationMessage(message.getConfirmation(), slaveNodeId);
                            break;
                        case BATCH_BUSINESS:
                            handleBatchBusinessMessage(message.getBatchBusiness(), slaveNodeId);
                            break;
                        default:
                            log.warn("Unknown message type: {}", header.getMessageType());
                    }
                    
                } catch (Exception e) {
                    log.error("Error processing message from slave", e);
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Error in replication server handler", cause);
            ctx.close();
        }
    }

    /**
         * 从节点连接封装
         */
        private record SlaveConnection(Channel channel, String slaveNodeId) {


        public void sendBatchReplicationRequest(ByteBuf batchMessage) {
            if (channel == null || !channel.isActive()) {
                log.warn("Channel is not active for slave: {}", slaveNodeId);
                return;
            }
            
            try {
                // 复制消息以避免多次发送时的问题
                ByteBuf messageCopy = batchMessage.copy();
                
                // 发送批处理消息
                channel.writeAndFlush(messageCopy).addListener(future -> {
                    if (future.isSuccess()) {
                        log.debug("Successfully sent batch business message to slave {}", slaveNodeId);
                    } else {
                        log.error("Failed to send batch business message to slave {}", 
                                slaveNodeId, future.cause());
                        // 注意：这里无法直接访问 replicationState，需要在外部处理
                    }
                });
                
            } catch (Exception e) {
                log.error("Exception while sending batch business message to slave {}", 
                        slaveNodeId, e);
                // 注意：这里无法直接访问 replicationState，需要在外部处理
            }
        }

        public void close() {
                if (channel != null && channel.isActive()) {
                    channel.close();
                }
            }
        }

}