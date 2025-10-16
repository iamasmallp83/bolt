package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.replication.ReplicationProto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 最简单的双向复制通信测试
 */
public class MinimalReplicationTest {

    private static final int PORT = 9091;

    private static ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) throws Exception {
        System.out.println("=== Replication Test ===\n");

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
                9092,  // prometheusPort
                true,  // isMaster
                "localhost", // masterHost
                9091,  // masterReplicationPort
                9092,  // slaveReplicationPort
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "master-journal", // journalFilePath
                false, // isBinary
                3000    // snapshotInterval
        );
        // 启动服务器
//        Server server = ServerBuilder.forPort(PORT)
//                .addService(new ReplicationServer(masterConfig, null))
//                .build();
//                .start();

        System.out.println("Server started on port " + PORT);

        // 等待服务器启动
        Thread.sleep(1000);

        // 启动客户端
        BoltConfig slaveConfig = new BoltConfig(
                1,     // nodeId
                "/Users/stam/Source/Java/bolt/slave", // boltHome
                19090,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9092,  // prometheusPort
                true,  // isMaster
                "localhost", // masterHost
                9091,  // masterReplicationPort
                9092,  // slaveReplicationPort
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "journal", // journalFilePath
                false, // isBinary
                3000    // snapshotInterval
        );

        ReplicationClient client = new ReplicationClient(7, "localhost", PORT, slaveConfig);
        client.start();
        Thread.sleep(60000);
    }
}
