package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.SlaveServer;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

/**
 * Bolt从节点 - 负责连接到主节点并接收复制请求
 * 新架构：JournalHandler -> SequencerDispatcher
 * SequencerDisruptor事件来源：GrpcReplicationClient
 */
public class BoltSlave extends BoltBase {

    private final SlaveServer slaveServer;

    public BoltSlave(BoltConfig config) {
        super(config);
        this.slaveServer = new SlaveServer(config, this.getEnvoyServer().getSequencerRingBuffer());
    }

    @Override
    protected void addReplicationServices(NettyServerBuilder builder) {
    }

    @Override
    protected void startNodeSpecificServices() throws IOException, InterruptedException {
    }

    @Override
    protected void stopNodeSpecificServices() {
        try {
            slaveServer.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
