package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationProto.*;
import com.google.common.util.concurrent.MoreExecutors;
import com.lmax.disruptor.RingBuffer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 从节点服务器
 */
@Slf4j
public class SlaveServer {

    private final BoltConfig config;
    private RingBuffer<NexusWrapper> sequencerRingBuffer;
    private Server server;

    public SlaveServer(BoltConfig config, RingBuffer<NexusWrapper> sequencerRingBuffer) {
        this.config = config;
        this.sequencerRingBuffer = sequencerRingBuffer;
        try {
            start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 启动从节点服务器
     */
    public void start() throws IOException {
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("SlaveServer-boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("SlaveServer-worker", true));
        ReplicationSlaveServiceImpl replicationSlaveService = new ReplicationSlaveServiceImpl(config, sequencerRingBuffer);
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(config.slaveReplicationPort())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(replicationSlaveService)
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(60, java.util.concurrent.TimeUnit.SECONDS); // 增加到60秒

        server = builder.build();
        server.start();
        replicationSlaveService.getSlaveSyncManager().start();
        log.info("Slave server started, listening on port {}", config.slaveReplicationPort());

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down slave server");
            try {
                SlaveServer.this.stop();
            } catch (InterruptedException e) {
                log.error("Error shutting down slave server", e);
                Thread.currentThread().interrupt();
            }
        }));
    }

    /**
     * 停止从节点服务器
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }

    }

    /**
     * 等待服务器终止
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
