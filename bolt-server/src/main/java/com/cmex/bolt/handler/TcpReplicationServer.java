package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
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
    private final ConfirmHandler confirmHandler; // 用于处理确认响应
    private final ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, SlaveConnection> slaveConnections = new ConcurrentHashMap<>();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    // TCP协议常量 - 现在使用 Protocol Buffers
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 1;

    public TcpReplicationServer(int port, ReplicationState replicationState, ConfirmHandler confirmHandler) {
        this.port = port;
        this.replicationState = replicationState;
        this.confirmHandler = confirmHandler;
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
     * 发送复制请求到指定从节点
     */
    public void sendReplicationRequest(String slaveNodeId, NexusWrapper wrapper, long sequence) {
        SlaveConnection connection = slaveConnections.get(slaveNodeId);
        if (connection == null) {
            log.warn("No connection found for slave: {}", slaveNodeId);
            return;
        }

        try {
            // 发送复制请求
            connection.sendReplicationRequest(wrapper, sequence);
            log.debug("Sent replication request to slave {} - sequence: {}", slaveNodeId, sequence);
        } catch (Exception e) {
            log.error("Failed to send replication request to slave {} - sequence: {}", slaveNodeId, sequence, e);
            replicationState.setSlaveConnected(slaveNodeId, false);
        }
    }

    /**
     * 处理从节点确认响应
     */
    public void handleSlaveConfirmation(long sequence, String slaveNodeId) {
        log.debug("Received confirmation from slave {} - sequence: {}", slaveNodeId, sequence);

        // 通知ConfirmHandler
        if (confirmHandler != null) {
            confirmHandler.handleSlaveConfirmation(sequence, slaveNodeId);
        }
    }
    
    /**
     * 发送确认消息到主节点（从节点使用）
     */
    public static void sendConfirmation(Channel channel, long sequence, String nodeId, boolean success, String errorMessage) {
        if (channel == null || !channel.isActive()) {
            log.warn("Channel is not active for sending confirmation");
            return;
        }
        
        try {
            // 使用新的 Protocol Buffers 协议编码确认消息
            ByteBuf message = ReplicationProtocolUtils.encodeConfirmationMessage(sequence, nodeId, success, errorMessage);
            
            // 发送消息
            channel.writeAndFlush(message).addListener(future -> {
                if (future.isSuccess()) {
                    log.debug("Successfully sent confirmation - sequence: {}, nodeId: {}", sequence, nodeId);
                } else {
                    log.error("Failed to send confirmation - sequence: {}, nodeId: {}", sequence, nodeId, future.cause());
                }
            });
            
        } catch (Exception e) {
            log.error("Exception while sending confirmation - sequence: {}, nodeId: {}", sequence, nodeId, e);
        }
    }
    
    /**
     * 处理注册消息
     */
    private void handleRegisterMessage(ReplicationProto.RegisterMessage registerMessage, String slaveNodeId) {
        log.info("Received register message from slave: {} - nodeId: {}, host: {}, port: {}", 
                slaveNodeId, registerMessage.getNodeId(), registerMessage.getHost(), registerMessage.getPort());
        
        // 注册从节点到ReplicationState
        replicationState.registerSlave(registerMessage.getNodeId(), registerMessage.getHost(), registerMessage.getPort());
        replicationState.setSlaveConnected(registerMessage.getNodeId(), true);
        
        // 注册到ConfirmHandler
        if (confirmHandler != null) {
            confirmHandler.addSlave(registerMessage.getNodeId());
        }
        
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
        
        // 通知ConfirmHandler
        if (confirmHandler != null) {
            confirmHandler.handleSlaveConfirmation(confirmationMessage.getSequence(), confirmationMessage.getNodeId());
        }
    }
    
    /**
     * 发送注册响应
     */
    private void sendRegisterResponse(String slaveNodeId, String assignedNodeId, boolean success, String message) {
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
            // 从slaveNodeId中提取host和port信息
            String[] parts = slaveNodeId.replace("/", "").split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            replicationState.registerSlave(slaveNodeId, host, port);
            replicationState.setSlaveConnected(slaveNodeId, true);

            // 注册到ConfirmHandler
            if (confirmHandler != null) {
                confirmHandler.addSlave(slaveNodeId);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            String slaveNodeId = ctx.channel().remoteAddress().toString();
            slaveConnections.remove(slaveNodeId);

            log.info("Slave disconnected: {}", slaveNodeId);
            replicationState.setSlaveConnected(slaveNodeId, false);
            replicationState.unregisterSlave(slaveNodeId);

            // 从ConfirmHandler中移除
            if (confirmHandler != null) {
                confirmHandler.removeSlave(slaveNodeId);
            }
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

        public void sendReplicationRequest(NexusWrapper wrapper, long sequence) {
            if (channel == null || !channel.isActive()) {
                log.warn("Channel is not active for slave: {}", slaveNodeId);
                return;
            }
            
            try {
                // 使用新的 Protocol Buffers 协议编码业务消息
                ByteBuf message = ReplicationProtocolUtils.encodeBusinessMessage(wrapper, sequence);
                
                // 发送消息
                channel.writeAndFlush(message).addListener(future -> {
                    if (future.isSuccess()) {
                        log.debug("Successfully sent business message to slave {} - sequence: {}", 
                                slaveNodeId, sequence);
                    } else {
                        log.error("Failed to send business message to slave {} - sequence: {}", 
                                slaveNodeId, sequence, future.cause());
                        // 注意：这里无法直接访问 replicationState，需要在外部处理
                    }
                });
                
            } catch (Exception e) {
                log.error("Exception while sending business message to slave {} - sequence: {}", 
                        slaveNodeId, sequence, e);
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