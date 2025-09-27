package com.cmex.bolt.replication;

import com.cmex.bolt.Bolt;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.handler.ReplicationClient;

public class BoltSlave {
    public static void main(String[] args) throws Exception {
        // 设置gRPC系统属性
        System.setProperty("io.grpc.internal.DnsNameResolverProvider.enable_service_config", "false");
        
        // 创建从节点配置
        BoltConfig slaveConfig = new BoltConfig(
                9093,  // port
                false, // isProd
                4,     // group
                1024,  // sequencerSize
                512,   // matchingSize
                512,   // responseSize
                false, // enablePrometheus
                9094,  // prometheusPort
                "slave-journal", // journalFilePath
                false, // isBinary
                false, // isMaster
                "127.0.0.1", // masterHost
                9092,  // masterPort - 指向master的复制服务端口
                9095,  // replicationPort
                true,  // enableReplication
                100,   // batchSize
                5000,  // batchTimeoutMs
                true,  // enableJournal
                "/Users/stam/Source/Java/bolt/slave" // boltHome
        );
        
        Bolt slave = new Bolt(slaveConfig);
        
        // 在后台线程中启动slave服务
        Thread slaveThread = new Thread(() -> {
            try {
                slave.start();
            } catch (Exception e) {
                System.err.println("Failed to start slave service: " + e.getMessage());
                e.printStackTrace();
            }
        }, "slave-service");
        slaveThread.setDaemon(true);
        slaveThread.start();
        
        // 等待服务启动完成
        Thread.sleep(3000);
        
        // 连接到master的复制服务
        try {
            System.out.println("Attempting to connect to master replication service at " + 
                    slaveConfig.masterHost() + ":" + slaveConfig.masterPort());
            
            ReplicationClient replicationClient = new ReplicationClient(
                    slaveConfig.masterHost(), 
                    slaveConfig.masterPort(), 
                    "slave-1"
            );
            
            // 连接到master
            replicationClient.connect();
            
            // 注册slave到master
            replicationClient.registerSlave("127.0.0.1", 9093, 0);
            
            System.out.println("Successfully connected and registered slave to master");
            
            // 启动心跳线程
            startHeartbeatThread(replicationClient);
            
            // 保持主线程运行
            slaveThread.join();
            
        } catch (Exception e) {
            System.err.println("Failed to connect to master: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void startHeartbeatThread(ReplicationClient client) {
        Thread heartbeatThread = new Thread(() -> {
            while (client.isConnected()) {
                try {
                    client.sendHeartbeat();
                    Thread.sleep(5000); // 每5秒发送一次心跳
                } catch (Exception e) {
                    System.err.println("Heartbeat failed: " + e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }
        }, "slave-heartbeat");
        
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }
}
