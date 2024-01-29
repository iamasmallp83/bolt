package com.cmex.bolt.spot;

import com.cmex.bolt.spot.grpc.SpotServiceImpl;
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

public class SpotServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        nettyServer(9090);
    }

    private static void nettyServer(int port) throws IOException, InterruptedException {
        final Server server = newNettyServer(port);
        server.start();
        System.out.println("Netty Config Server started");
        shutdown(server);
    }

    private static Server newNettyServer(int port) {
        // On Linux it can, possibly, be improved by using
        // io.netty.channel.epoll.EpollEventLoopGroup
        // io.netty.channel.epoll.EpollServerSocketChannel
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("worker", true));
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(port)
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(new SpotServiceImpl())
                .flowControlWindow(NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW);
        builder.executor(MoreExecutors.directExecutor());
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
