package com.cmex.bolt.replay;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.handler.JournalReplayer;
import com.cmex.bolt.recovery.SnapshotRecovery;
import com.cmex.bolt.recovery.SnapshotData;
import com.lmax.disruptor.RingBuffer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 主节点数据重放策略
 * 主节点从本地snapshot和journal文件恢复数据
 */
public class MasterNodeReplayStrategy implements DataReplayStrategy {
    private static final Logger log = LoggerFactory.getLogger(MasterNodeReplayStrategy.class);

    private final BoltConfig config;

    public MasterNodeReplayStrategy(BoltConfig config) {
        this.config = config;
    }

    @Override
    public SnapshotData replaySnapshot() {
        try {
            log.info("Master node: replaying from local snapshot");
            SnapshotRecovery snapshotRecovery = new SnapshotRecovery(config);
            return snapshotRecovery.recoverFromSnapshot();
        } catch (IOException e) {
            log.error("Master node: failed to replay snapshot", e);
        }
        return null;
    }

    @Override
    public long replayJournal() {
        log.info("Master node: replaying from local journal");
        // 这里需要RingBuffer，将通过回调方式设置
        return 0; // 实际实现将在EnvoyServer中完成
    }

}
