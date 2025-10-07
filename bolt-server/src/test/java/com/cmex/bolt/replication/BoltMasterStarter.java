package com.cmex.bolt.replication;

import java.io.IOException;

import com.cmex.bolt.BoltMaster;
import com.cmex.bolt.core.BoltConfig;

public class BoltMasterStarter {
    public static void main(String[] args) throws IOException, InterruptedException {
        BoltConfig masterConfig = new BoltConfig(
                1,     // nodeId
                "/Users/stam/Source/Java/bolt/master", // boltHome
                9090,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9091,  // prometheusPort
                true,  // isMaster
                "localhost", // masterHost
                9090,  // masterPort
                9092,  // replicationPort
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "master-journal", // journalFilePath
                false, // isBinary
                3000    // snapshotInterval
        );
        BoltMaster master = new BoltMaster(masterConfig);
        master.start();
    }
}
