package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.NettyMetricsServer;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Bolt基础类 - 包含主从节点共同的组件和逻辑
 */
@Slf4j
@Getter
public abstract class BoltBase {
    
    protected final BoltConfig config;
    protected final EnvoyServer envoyServer;
    protected Server nettyServer;
    protected NettyMetricsServer metricsServer;
    
    public BoltBase(BoltConfig config) {
        this.config = config;
        log.info("Creating {} with config: port={}, group={}", 
                getClass().getSimpleName(), config.port(), config.group());
        this.envoyServer = new EnvoyServer(config);
        log.info("EnvoyServer created successfully");
    }
    
    /**
     * 启动Bolt实例
     */
    public void start() throws IOException, InterruptedException {
        log.info("Starting {} with config: {}", getClass().getSimpleName(), config);
        
        // 启动gRPC服务器
        this.nettyServer = createNettyServer();
        
        // 启动Prometheus监控（如果启用）
        if (config.enablePrometheus()) {
            this.metricsServer = new NettyMetricsServer(config.prometheusPort());
            this.metricsServer.start();
            log.info("Prometheus metrics server started on port {}", config.prometheusPort());
        }
        
        // 启动特定于节点类型的服务
        startNodeSpecificServices();
        
        // 启动gRPC服务器
        nettyServer.start();
        log.info("{} started successfully", getClass().getSimpleName());
        
        // 等待关闭
        awaitShutdown();
    }
    
    /**
     * 创建Netty gRPC服务器
     */
    protected Server createNettyServer() {
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
    
    /**
     * 启动特定于节点类型的服务 - 子类实现
     */
    protected abstract void startNodeSpecificServices() throws IOException, InterruptedException;
    
    /**
     * 停止特定于节点类型的服务 - 子类实现
     */
    protected abstract void stopNodeSpecificServices();
    
    /**
     * 等待关闭
     */
    private void awaitShutdown() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("{} shutting down...", getClass().getSimpleName());
                
                // 停止gRPC服务器
                if (nettyServer != null) {
                    nettyServer.shutdown();
                }
                
                // 停止节点特定服务
                stopNodeSpecificServices();
                
                // 停止监控服务器
                if (metricsServer != null) {
                    metricsServer.shutdown();
                }
                
                // 停止Envoy服务器
                if (envoyServer != null) {
                    envoyServer.shutdown();
                }
                
                log.info("{} shutdown completed", getClass().getSimpleName());
            } catch (Exception e) {
                log.error("Error during {} shutdown: {}", getClass().getSimpleName(), e.getMessage(), e);
            }
        }));
        
        if (nettyServer != null) {
            nettyServer.awaitTermination();
        }
    }
    
    /**
     * 检查服务是否运行
     */
    public boolean isRunning() {
        return nettyServer != null && !nettyServer.isShutdown();
    }
    
    /**
     * 创建目录结构
     */
    protected static void createDirectories(BoltConfig config) {
        try {
            String logsDir = System.getProperty("LOGS_DIR", config.boltHome() + "/logs");
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(logsDir));
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(config.journalDir()));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to create directories: " + e.getMessage(), e);
        }
    }
}
