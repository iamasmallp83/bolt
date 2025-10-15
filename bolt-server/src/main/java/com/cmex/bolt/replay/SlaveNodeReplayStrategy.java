package com.cmex.bolt.replay;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.recovery.SnapshotData;
import com.cmex.bolt.recovery.SnapshotRecovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 从节点数据重放策略
 * 从节点不需要本地数据恢复，等待主节点同步数据
 */
public class SlaveNodeReplayStrategy implements DataReplayStrategy {
    private static final Logger log = LoggerFactory.getLogger(SlaveNodeReplayStrategy.class);

    private final BoltConfig config;


    public SlaveNodeReplayStrategy(BoltConfig config) {
        this.config = config;
    }

    @Override
    public SnapshotData replaySnapshot() {
        try {
            log.info("Slave node: replaying from local snapshot");
            SnapshotRecovery snapshotRecovery = new SnapshotRecovery(config);
            return snapshotRecovery.recoverFromSnapshot();
        } catch (IOException e) {
            log.error("Slave node: failed to replay snapshot", e);
        }
        return null;
    }

    @Override
    public long replayJournal() {
        log.info("Slave node: no local journal replay, will receive from master");
        // 从节点不需要本地journal重放
        return 0;
    }

    /**
     * 创建空的Snapshot数据
     */
    public SnapshotData createEmptySnapshotData() {
        // 这里需要根据实际的SnapshotData构造函数来实现
        // 暂时返回null，实际实现时需要创建空的SnapshotData
        return null;
    }
}
