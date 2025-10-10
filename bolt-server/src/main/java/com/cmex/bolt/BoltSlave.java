package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.SlaveReplicationManager;
import com.cmex.bolt.replication.SlaveReplicationServiceImpl;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;

/**
 * Bolt从节点 - 负责连接到主节点并接收复制请求
 * 新架构：JournalHandler -> SequencerDispatcher
 * SequencerDisruptor事件来源：GrpcReplicationClient
 */
public class BoltSlave extends BoltBase {

    private final SlaveReplicationManager slaveReplicationManager;
    private Server replicationServer;

    public BoltSlave(BoltConfig config) {
        super(config);
        this.slaveReplicationManager = new SlaveReplicationManager(config, envoyServer.getSequencerRingBuffer());
    }

    @Override
    protected void addReplicationServices(NettyServerBuilder builder) {
        // 主业务服务器不添加复制服务，复制服务在单独的端口提供
        System.out.println("Business server does not provide replication services");
    }

    @Override
    protected void startNodeSpecificServices() throws IOException, InterruptedException {
        // 启动从节点复制管理器
        slaveReplicationManager.start();
        System.out.println("Slave replication manager started");
        
        // 启动复制服务服务器
        startReplicationServer();
        System.out.println("Slave replication server started on port " + config.slaveReplicationPort());
    }

    @Override
    protected void stopNodeSpecificServices() {
        // 停止复制服务服务器
        if (replicationServer != null && !replicationServer.isShutdown()) {
            replicationServer.shutdown();
            try {
                replicationServer.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Slave replication server stopped");
        }
        
        // 停止从节点复制管理器
        if (slaveReplicationManager != null) {
            slaveReplicationManager.stop();
            System.out.println("Slave replication manager stopped");
        }
    }

    /**
     * 启动复制服务服务器
     */
    private void startReplicationServer() throws IOException {
        final EventLoopGroup boss = new NioEventLoopGroup(1, new DefaultThreadFactory("replication-boss", true));
        final EventLoopGroup worker = new NioEventLoopGroup(0, new DefaultThreadFactory("replication-worker", true));
        
        NettyServerBuilder builder = NettyServerBuilder
                .forPort(config.slaveReplicationPort())
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .channelType(NioServerSocketChannel.class)
                .addService(new SlaveReplicationServiceImpl(slaveReplicationManager))
                .maxInboundMessageSize(16 * 1024 * 1024) // 16MB
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS)
                .executor(MoreExecutors.directExecutor());
        
        replicationServer = builder.build();
        replicationServer.start();
        
        System.out.println("SlaveReplicationService started on port " + config.slaveReplicationPort());
    }

}
