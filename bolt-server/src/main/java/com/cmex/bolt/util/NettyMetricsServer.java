package com.cmex.bolt.util;

import io.grpc.netty.shaded.io.netty.bootstrap.ServerBootstrap;
import io.grpc.netty.shaded.io.netty.channel.*;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 基于Netty的Prometheus指标服务器
 * 复用现有的EventLoopGroup，避免额外的线程开销
 */
public class NettyMetricsServer {
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel serverChannel;
    private volatile boolean running = false;
    
    public NettyMetricsServer(int port) {
        this.port = port;
        // 创建独立的EventLoopGroup用于指标服务器
        // 这样可以避免与gRPC服务器共享线程池，减少相互影响
        this.bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("metrics-boss", true));
        this.workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("metrics-worker", true));
    }
    
    /**
     * 启动指标服务器
     */
    public void start() throws IOException {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            
                            // HTTP编解码器
                            pipeline.addLast("http-codec", new HttpServerCodec());
                            pipeline.addLast("http-aggregator", new HttpObjectAggregator(65536));
                            pipeline.addLast("http-expect-continue", new HttpServerExpectContinueHandler());
                            
                            // 指标处理器
                            pipeline.addLast("metrics-handler", new NettyMetricsHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            
            ChannelFuture future = bootstrap.bind(new InetSocketAddress(port)).sync();
            serverChannel = future.channel();
            running = true;
            
            System.out.println("Netty metrics server started on port " + port);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to start metrics server", e);
        } catch (Exception e) {
            throw new IOException("Failed to start metrics server", e);
        }
    }
    
    /**
     * 关闭指标服务器
     */
    public void shutdown() {
        if (serverChannel != null) {
            try {
                serverChannel.close().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        try {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        running = false;
        System.out.println("Netty metrics server stopped");
    }
    
    /**
     * 检查服务器是否正在运行
     */
    public boolean isRunning() {
        return running && serverChannel != null && serverChannel.isActive();
    }
    
    /**
     * 获取服务器端口
     */
    public int getPort() {
        return port;
    }
    
    /**
     * 获取运行状态
     */
    public boolean getRunning() {
        return running;
    }
}
