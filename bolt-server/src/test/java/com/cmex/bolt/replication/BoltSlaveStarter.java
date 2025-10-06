package com.cmex.bolt.replication;

import com.cmex.bolt.BoltSlave;
import com.cmex.bolt.core.BoltConfig;

public class BoltSlaveStarter {
    public static void main(String[] args) throws Exception {
        // 创建从节点配置
        BoltConfig slaveConfig = new BoltConfig(
                2,     // nodeId
                "/Users/ly/Source/Java/bolt/slave", // boltHome
                9093,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9094,  // prometheusPort
                false, // isMaster
                "127.0.0.1", // masterHost
                9092,  // masterPort - 指向master的TCP复制服务端口
                9095,  // replicationPort
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "slave-journal", // journalFilePath
                false, // isBinary
                30    // snapshotInterval
        );
        
        BoltSlave slave = new BoltSlave(slaveConfig);
        
        System.out.println("Starting slave node with TCP replication to master at " + 
                slaveConfig.masterHost() + ":" + slaveConfig.masterPort());
        
        slave.start();
    }
}
