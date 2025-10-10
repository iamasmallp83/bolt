package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.recovery.SnapshotReader;
import com.cmex.bolt.handler.JournalReplayer;
import com.cmex.bolt.replication.MasterReplicationServiceImpl;
import com.cmex.bolt.replication.MasterServer;
import com.cmex.bolt.replication.ReplicationManager;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.InterruptedException;

/**
 * Bolt主节点 - 负责处理gRPC复制服务
 * 新架构：JournalHandler -> ReplicationHandler -> SequencerDispatcher
 */
@Slf4j
public class BoltMaster extends BoltBase {

    private final MasterServer masterServer;

    private final ReplicationManager replicationManager;

    public BoltMaster(BoltConfig config) {
        super(config);

        // 验证主节点配置
        if (!config.isMaster()) {
            throw new IllegalArgumentException("BoltMaster requires master configuration");
        }

        // 创建复制管理器
        this.masterServer = new MasterServer(config.masterReplicationPort(), replicationManager);
        this.replicationManager = new ReplicationManager();
        try {
            this.masterServer.start();
        } catch (IOException e) {
            throw new RuntimeException("Can not start master server");
        }


        // 创建gRPC复制服务
        SnapshotReader snapshotReader = new SnapshotReader(config);
        JournalReplayer journalReplayer = new JournalReplayer(envoyServer.getSequencerRingBuffer(), config);
        log.info("BoltMaster initialized with EnvoyServer and ReplicationManager");
    }

    @Override
    protected void addReplicationServices(NettyServerBuilder builder) {
        // 添加主节点复制服务
        MasterReplicationServiceImpl masterService = new MasterReplicationServiceImpl(replicationManager);
        builder.addService(masterService);
        log.info("Added MasterReplicationService to gRPC server");
    }

    @Override
    protected void startNodeSpecificServices() {
        try {
            // 启动复制管理器
            replicationManager.start();
            log.info("Master replication manager started");
        } catch (Exception e) {
            log.error("Failed to initialize master replication service", e);
            throw new RuntimeException("Failed to initialize master replication service", e);
        }
    }

    @Override
    protected void stopNodeSpecificServices() {
        if (replicationManager != null) {
            replicationManager.stop();
            log.info("Master replication manager stopped");
        }
    }

    /**
     * 获取复制管理器
     */
    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    /**
     * 主节点入口点
     */
    public static void main(String[] args) {
        try {
            BoltConfig config = BoltConfig.DEFAULT; // 使用默认配置
            BoltMaster master = new BoltMaster(config);
            master.start();

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down BoltMaster...");
                // BoltBase已经处理了关闭逻辑
            }));

            // 保持运行
            master.awaitTermination();

        } catch (Exception e) {
            log.error("Failed to start BoltMaster", e);
            System.exit(1);
        }
    }

    /**
     * 等待终止
     */
    private void awaitTermination() throws InterruptedException {
        if (nettyServer != null) {
            nettyServer.awaitTermination();
        }
    }
}
