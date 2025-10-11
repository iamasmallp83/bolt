package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.handler.JournalReplayer;
import com.cmex.bolt.recovery.SnapshotReader;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Bolt主节点 - 负责处理gRPC复制服务
 * 新架构：JournalHandler -> ReplicationHandler -> SequencerDispatcher
 */
@Slf4j
public class BoltMaster extends BoltBase {

    public BoltMaster(BoltConfig config) {
        super(config);

        // 验证主节点配置
        if (!config.isMaster()) {
            throw new IllegalArgumentException("BoltMaster requires master configuration");
        }

        // 创建gRPC复制服务
        SnapshotReader snapshotReader = new SnapshotReader(config);
        JournalReplayer journalReplayer = new JournalReplayer(envoyServer.getSequencerRingBuffer(), config);
        log.info("BoltMaster initialized with EnvoyServer and MasterServer");
    }

    @Override
    protected void addReplicationServices(NettyServerBuilder builder) {
    }

    @Override
    protected void startNodeSpecificServices() {
    }

    @Override
    protected void stopNodeSpecificServices() {
    }

    /*
     * 等待终止
     */
    private void awaitTermination() throws InterruptedException {
        if (nettyServer != null) {
            nettyServer.awaitTermination();
        }
    }
}
