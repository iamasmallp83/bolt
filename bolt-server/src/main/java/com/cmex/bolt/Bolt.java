package com.cmex.bolt;

import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.core.BoltConfig;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class Bolt {

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0) {
            nettyServer(BoltConfig.DEFAULT);
        } else {
            try {
                int port = Integer.parseInt(args[0]);
                boolean isProd = Boolean.parseBoolean(args[1]);
                int group = Integer.parseInt(args[2]);
                int sequencerSize = Integer.parseInt(args[3]);
                int matchingSize = Integer.parseInt(args[4]);
                int responseSize = Integer.parseInt(args[5]);
                BoltConfig config = new BoltConfig(port, isProd, group, sequencerSize, matchingSize, responseSize);
                nettyServer(config);
            } catch (NumberFormatException e) {
                for (String arg : args) {
                    System.out.print("Your args is ");
                    System.out.print(arg + " ");
                    System.out.println();
                }
                System.out.println("usage {port} {isProduction} {group} {sequencerSize} {matchingSize} {responseSize}");
            } catch (IOException | InterruptedException e) {
            }
        }
    }

    private static void nettyServer(BoltConfig boltConfig) throws IOException, InterruptedException {
        final Server server = newNettyServer(boltConfig);
        System.out.println("Start Bolt with " + boltConfig);
        server.start();
        System.out.println("Bolt started");
        shutdown(server);
    }

    private static Server newNettyServer(BoltConfig boltConfig) {
        // On Linux it can, possibly, be improved by using
        // io.netty.channel.epoll.EpollEventLoopGroup
        // io.netty.channel.epoll.EpollServerSocketChannel
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("worker", true));
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(boltConfig.port())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(new EnvoyServer(boltConfig))
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


    private static void shutdown(Server server) throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("Server shutting down");
                server.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        server.awaitTermination();
    }

}
