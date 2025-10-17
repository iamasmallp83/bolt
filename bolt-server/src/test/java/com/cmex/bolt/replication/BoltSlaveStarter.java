package com.cmex.bolt.replication;

import com.cmex.bolt.BoltSlave;
import com.cmex.bolt.core.BoltConfig;

public class BoltSlaveStarter {
    public static void main(String[] args) throws Exception {
        // 创建从节点配置
        BoltConfig slaveConfig = new BoltConfig(
                100,     // nodeId
                "/Users/ly/Source/Java/bolt/slave", // boltHome
                19090,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9093,  // prometheusPort
                false, // isMaster
                "127.0.0.1", // masterHost
                9091,  // masterReplicationPort
                true,  // enableJournal
                false, // isBinary
                3000    // snapshotInterval
        );
        
        BoltSlave slave = new BoltSlave(slaveConfig);
        
    }
}
