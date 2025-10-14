package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.NettyMetricsServer;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Bolt公共组件类
 * 包含主从节点共同的组件和逻辑，使用组合模式
 */
@Getter
public class BoltCore {
    private static final Logger log = LoggerFactory.getLogger(BoltCore.class);
    
    private final BoltConfig config;
    private final EnvoyServer envoyServer;
    private Server nettyServer;
    private NettyMetricsServer metricsServer;
    
    public BoltCore(BoltConfig config) {
        this.config = config;
        log.info("Creating BoltCore with config: port={}, group={}", 
                config.port(), config.group());
        this.envoyServer = new EnvoyServer(config);
        log.info("BoltCore initialized with EnvoyServer");
    }
    
    /**
     * 启动基础组件
     */
    public void start() throws IOException, InterruptedException {
        log.info("Starting BoltCore with config: {}", config);
        
        // 启动gRPC服务器
        this.nettyServer = createNettyServer();
        
        // 启动Prometheus监控（如果启用）
        if (config.enablePrometheus()) {
            this.metricsServer = new NettyMetricsServer(config.prometheusPort());
            this.metricsServer.start();
            log.info("Prometheus metrics server started on port {}", config.prometheusPort());
        }
        
        // 启动gRPC服务器
        nettyServer.start();
        log.info("BoltCore started successfully");
    }
    
    /**
     * 停止基础组件
     */
    public void stop() {
        log.info("Stopping BoltCore");
        
        // 停止gRPC服务器
        if (nettyServer != null) {
            nettyServer.shutdown();
        }
        
        // 停止监控服务器
        if (metricsServer != null) {
            metricsServer.shutdown();
        }
        
        // 停止Envoy服务器
        if (envoyServer != null) {
            envoyServer.shutdown();
        }
        
        log.info("BoltCore stopped");
    }
    
    /**
     * 等待关闭
     */
    public void awaitShutdown() throws InterruptedException {
        if (nettyServer != null) {
            nettyServer.awaitTermination();
        }
    }
    
    /**
     * 创建Netty gRPC服务器
     */
    private Server createNettyServer() {
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("worker", true));
        
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(config.port())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(envoyServer)
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(30, TimeUnit.SECONDS);
        
        // 使用worker执行操作
        builder.executor(MoreExecutors.directExecutor());
        
        return builder.build();
    }
}
