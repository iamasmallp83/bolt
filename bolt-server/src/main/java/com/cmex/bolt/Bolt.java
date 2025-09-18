package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.PrometheusMetricsServer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class Bolt {

    // Getter methods for accessing internal state
    @Getter
    private final BoltConfig config;
    @Getter
    private Server nettyServer;
    @Getter
    private EnvoyServer envoyServer;
    @Getter
    private PrometheusMetricsServer prometheusServer;

    public Bolt(BoltConfig config) {
        this.config = config;
        this.envoyServer = new EnvoyServer(config);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BoltConfig config;
        if (args.length == 0) {
            config = BoltConfig.DEFAULT;
        } else if (args.length == 8) {
            try {
                int port = Integer.parseInt(args[0]);
                boolean isProd = Boolean.parseBoolean(args[1]);
                int group = Integer.parseInt(args[2]);
                int sequencerSize = Integer.parseInt(args[3]);
                int matchingSize = Integer.parseInt(args[4]);
                int responseSize = Integer.parseInt(args[5]);
                boolean enablePrometheus = Boolean.parseBoolean(args[6]);
                int prometheusPort = Integer.parseInt(args[7]);
                config = new BoltConfig(port, isProd, group, sequencerSize, matchingSize, responseSize, enablePrometheus, prometheusPort);
            } catch (NumberFormatException e) {
                printUsage();
                return;
            }
        } else {
            printUsage();
            return;
        }
        Bolt bolt = new Bolt(config);
        bolt.start();
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  java -jar bolt.jar  # 使用默认配置");
        System.out.println("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus}");
        System.out.println("Examples:");
        System.out.println("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091");
    }

    public void start() throws IOException, InterruptedException {
        this.nettyServer = newNettyServer();
        this.prometheusServer = null;

        // 启动 Prometheus 监控（如果启用）
        if (config.enablePrometheus()) {
            this.prometheusServer = new PrometheusMetricsServer(config.prometheusPort());
        }
        System.out.println("Start Bolt with " + config);
        nettyServer.start();
        System.out.println("Bolt started");
        shutdown();
    }

    private Server newNettyServer() {
        // On Linux it can, possibly, be improved by using
        // io.netty.channel.epoll.EpollEventLoopGroup
        // io.netty.channel.epoll.EpollServerSocketChannel
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("worker", true));
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(config.port())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(envoyServer)
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW);
        //使用worker执行操作
        builder.executor(MoreExecutors.directExecutor());
        //创建线程池执行
//        builder.executor(getAsyncExecutor());
        return builder.build();
    }

    private static Executor getAsyncExecutor() {
        return new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                new ForkJoinPool.ForkJoinWorkerThreadFactory() {
                    final AtomicInteger num = new AtomicInteger();

                    @Override
                    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                        ForkJoinWorkerThread thread =
                                ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                        thread.setDaemon(true);
                        thread.setName("grpc-server-app-" + "-" + num.getAndIncrement());
                        return thread;
                    }
                }, UncaughtExceptionHandlers.systemExit(), true);
    }


    private void shutdown() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Server shutting down...");
                nettyServer.shutdown();
                if (prometheusServer != null) {
                    System.out.println("Prometheus metrics server shutting down...");
                    prometheusServer.shutdown();
                }
                System.out.println("Shutdown completed");
            } catch (Exception e) {
                System.err.println("Error during shutdown: " + e.getMessage());
                e.printStackTrace();
            }
        }));
        newNettyServer().awaitTermination();
    }

    public boolean isRunning() {
        return nettyServer != null && !nettyServer.isShutdown();
    }
}
