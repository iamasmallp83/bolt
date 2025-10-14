package com.cmex.bolt.replay;

import com.cmex.bolt.core.BoltConfig;

/**
 * 数据重放策略工厂
 * 根据配置创建相应的重放策略
 */
public class DataReplayStrategyFactory {
    
    /**
     * 根据配置创建数据重放策略
     * @param config 配置信息
     * @return 对应的重放策略
     */
    public static DataReplayStrategy createStrategy(BoltConfig config) {
        if (config.isMaster()) {
            return new MasterNodeReplayStrategy(config);
        } else {
            return new SlaveNodeReplayStrategy(config);
        }
    }
}
