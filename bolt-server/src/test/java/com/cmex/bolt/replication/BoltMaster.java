package com.cmex.bolt.replication;

import java.io.IOException;

import com.cmex.bolt.Bolt;
import com.cmex.bolt.core.BoltConfig;

public class BoltMaster {
    public static void main(String[] args) throws IOException, InterruptedException {
        BoltConfig masterConfig = new BoltConfig(
                9090,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9091,  // prometheusPort
                "master-journal", // journalFilePath
                false, // isBinary
                true,  // isMaster
                "localhost", // masterHost
                9090,  // masterPort
                9092,  // replicationPort
                true,  // enableReplication
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "/Users/stam/Source/Java/bolt/master" // boltHome
        );
        Bolt master = new Bolt(masterConfig);
        master.start();
    }
}
