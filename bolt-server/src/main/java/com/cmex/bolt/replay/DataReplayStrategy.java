package com.cmex.bolt.replay;

import com.cmex.bolt.recovery.SnapshotData;

/**
 * 数据重放策略接口
 * 定义主从节点不同的数据恢复和重放方式
 */
public interface DataReplayStrategy {
    
    /**
     * 重放Snapshot数据
     * @return 是否成功重放
     */
    SnapshotData replaySnapshot();
    
    /**
     * 重放Journal数据
     * @return 重放的最大ID
     */
    long replayJournal();
}
