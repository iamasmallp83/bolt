package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.handler.ReplicationHandler;
import com.cmex.bolt.replication.ReplicationServiceImpl;
import com.cmex.bolt.replication.ReplicationState;
import com.cmex.bolt.util.NettyMetricsServer;
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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Bolt {

    // Getter methods for accessing internal state
    @Getter
    private final BoltConfig config;
    @Getter
    private Server nettyServer;
    @Getter
    private Server replicationServer;
    @Getter
    private EnvoyServer envoyServer;
    @Getter
    private NettyMetricsServer metricsServer;

    public Bolt(BoltConfig config) {
        this.config = config;
        this.envoyServer = new EnvoyServer(config);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // 首先解析配置，然后立即设置系统属性
        BoltConfig config;
        if (args.length == 0) {
            config = BoltConfig.DEFAULT;
        } else if (args.length == 19) {
            try {
                int port = Integer.parseInt(args[0]);
                boolean isProd = Boolean.parseBoolean(args[1]);
                int group = Integer.parseInt(args[2]);
                int sequencerSize = Integer.parseInt(args[3]);
                int matchingSize = Integer.parseInt(args[4]);
                int responseSize = Integer.parseInt(args[5]);
                boolean enablePrometheus = Boolean.parseBoolean(args[6]);
                int prometheusPort = Integer.parseInt(args[7]);
                String journalFilePath = args[8];
                boolean isBinary = Boolean.parseBoolean(args[9]);
                boolean isMaster = Boolean.parseBoolean(args[10]);
                String masterHost = args[11];
                int masterPort = Integer.parseInt(args[12]);
                int replicationPort = Integer.parseInt(args[13]);
                boolean enableReplication = Boolean.parseBoolean(args[14]);
                int batchSize = Integer.parseInt(args[15]);
                int batchTimeoutMs = Integer.parseInt(args[16]);
                boolean enableJournal = Boolean.parseBoolean(args[17]);
                String boltHome = args[18];
                config = new BoltConfig(port, isProd, group, sequencerSize, matchingSize, responseSize, 
                    enablePrometheus, prometheusPort, journalFilePath, isBinary, isMaster, masterHost, 
                    masterPort, replicationPort, enableReplication, batchSize, batchTimeoutMs, enableJournal, boltHome);
            } catch (NumberFormatException e) {
                printUsage();
                return;
            }
        } else {
            printUsage();
            return;
        }
        
        // 根据isMaster设置logback配置文件
        String logbackConfig = config.isMaster() ? "master-logback.xml" : "slave-logback.xml";
        System.setProperty("logback.configurationFile", logbackConfig);
        
        // 创建必要的目录
        try {
            // 从系统属性获取日志目录，如果没有设置则使用默认值
            String logsDir = System.getProperty("LOGS_DIR", config.boltHome() + "/logs");
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(logsDir));
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(config.journalDir()));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to create directories: " + e.getMessage(), e);
        }
        
        Bolt bolt = new Bolt(config);
        bolt.start();
    }


    private static void printUsage() {
        log.info("Usage:");
        log.info("  java -jar bolt.jar  # 使用默认配置");
        log.info("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary}");
        log.info("  java -jar bolt.jar {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize} {prometheusPort} {enablePrometheus} {journalFilePath} {isBinary} {isMaster} {masterHost} {masterPort} {replicationPort} {enableReplication} {batchSize} {batchTimeoutMs} {enableJournal} {boltHome}");
        log.info("Logback Configuration:");
        log.info("  - Master mode: uses master-logback.xml (creates bolt-master.log)");
        log.info("  - Slave mode: uses slave-logback.xml (creates bolt-slave.log)");
        log.info("  - Override logs directory: -DLOGS_DIR=/path/to/logs");
        log.info("Examples:");
        log.info("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false");
        log.info("  java -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false false localhost 9090 9092 true 100 5000 true /path/to/bolt");
        log.info("  java -DLOGS_DIR=/var/log/bolt -jar bolt.jar 9090 true 4 1024 512 512 true 9091 journal.data false true localhost 9090 9092 true 100 5000 true /path/to/bolt");
    }

    public void start() throws IOException, InterruptedException {
        this.nettyServer = newNettyServer();
        this.metricsServer = null;
        this.replicationServer = null;

        // 启动 Prometheus 监控（如果启用）
        if (config.enablePrometheus()) {
            this.metricsServer = new NettyMetricsServer(config.prometheusPort());
            this.metricsServer.start();
        }
        
        // 启动复制服务（如果启用且是主节点）
        if (config.enableReplication() && config.isMaster()) {
            this.replicationServer = newReplicationServer();
            replicationServer.start();
            log.info("Replication server started on port {}", config.replicationPort());
        }
        
        log.info("Start Bolt with {}", config);
        nettyServer.start();
        log.info("Bolt started");
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
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                // 添加协议检测和错误处理
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS);
        
        // gRPC服务器不需要额外的HTTP处理器
        
        //使用worker执行操作
        builder.executor(MoreExecutors.directExecutor());
        //创建线程池执行
//        builder.executor(getAsyncExecutor());
        return builder.build();
    }
    
    private Server newReplicationServer() {
        log.info("Creating replication server on port {}", config.replicationPort());
        
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("replication-boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("replication-worker", true));
        
        // 直接创建复制服务实现
        log.debug("Creating ReplicationHandler and ReplicationServiceImpl");
        ReplicationHandler replicationHandler = new ReplicationHandler(config, new ReplicationState());
        ReplicationServiceImpl replicationService = new ReplicationServiceImpl(replicationHandler);
        
        log.debug("Configuring Netty server builder for replication port {}", config.replicationPort());
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(config.replicationPort())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(replicationService)
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW)
                // 添加协议检测和错误处理
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(30, TimeUnit.SECONDS);
        
        builder.executor(MoreExecutors.directExecutor());
        
        log.info("Replication server configured successfully on port {}", config.replicationPort());
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
                log.info("Server shutting down...");
                nettyServer.shutdown();
                if (replicationServer != null) {
                    log.info("Replication server shutting down...");
                    replicationServer.shutdown();
                }
                if (metricsServer != null) {
                    log.info("Metrics server shutting down...");
                    metricsServer.shutdown();
                }
                log.info("Shutdown completed");
            } catch (Exception e) {
                log.error("Error during shutdown: {}", e.getMessage(), e);
            }
        }));
        nettyServer.awaitTermination();
    }

    public boolean isRunning() {
        return nettyServer != null && !nettyServer.isShutdown() && 
               (replicationServer == null || !replicationServer.isShutdown());
    }
}
