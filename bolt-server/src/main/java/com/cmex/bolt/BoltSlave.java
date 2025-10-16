package com.cmex.bolt;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.ReplicationClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Bolt从节点 - 使用组合模式和依赖注入解耦
 * 负责启动从节点特有的服务：SlaveServer + SlaveSyncManager
 */
@Getter
@Slf4j
public class BoltSlave {
    // Getters
    private final BoltConfig config;
    private BoltCore core;

    public BoltSlave(BoltConfig config) {
        // 验证从节点配置
        if (config.isMaster()) {
            throw new IllegalArgumentException("BoltSlave requires slave configuration");
        }
        this.config = config;
        log.info("BoltSlave initialized with ReplicationContext");
        ReplicationClient client = new ReplicationClient(config);
        client.start();
    }

}