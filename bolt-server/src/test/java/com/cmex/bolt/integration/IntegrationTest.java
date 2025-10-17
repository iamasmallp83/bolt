package com.cmex.bolt.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import com.cmex.bolt.util.FakeStreamObserver;

/**
 * 简单的gRPC客户端测试类
 * 直接使用EnvoyServer实例进行测试，无需启动网络服务
 */
public class IntegrationTest {

    private static EnvoyServer envoyServer;

    @BeforeAll
    static void setUp() {
        // 创建BoltConfig配置
        BoltConfig config = new BoltConfig(
                1,                          // nodeId
                "./bolt-home",              // boltHome
                9090,                       // port
                false,                      // isProd
                4,                          // group
                1024,                       // sequencerSize
                512,                        // matchingSize
                512,                        // responseSize
                false,                      // enablePrometheus
                9091,                       // prometheusPort
                true,                       // isMaster
                "localhost",                // masterHost
                9090,                       // masterReplicationPort
                true,                       // enableJournal
                false,                      // isBinary
                300                         // snapshotInterval
        );

        // 创建EnvoyServer实例
//        envoyServer = new EnvoyServer(config);
        System.out.println("EnvoyServer实例已创建");
    }

    @Test
    void testIncreaseBalance() {
        System.out.println("\n=== 测试增加余额请求 ===");
        
        // 创建增加余额请求
        Envoy.IncreaseRequest request = Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1L)
                .setAccountId(1)
                .setAmount("100")
                .setCurrencyId(1)
                .build();

        System.out.println("发送增加余额请求: " + request);

        // 创建响应观察者
        FakeStreamObserver<Envoy.IncreaseResponse> responseObserver = FakeStreamObserver.of(response -> {
            System.out.println("收到增加余额响应:");
            System.out.println("  状态码: " + response.getCode());
            System.out.println("  消息: " + (response.hasMessage() ? response.getMessage() : "无"));
            if (response.hasData()) {
                Envoy.Balance balance = response.getData();
                System.out.println("  余额信息:");
                System.out.println("    货币: " + balance.getCurrency());
                System.out.println("    总额: " + balance.getValue());
                System.out.println("    可用: " + balance.getAvailable());
                System.out.println("    冻结: " + balance.getFrozen());
            }
        });

        // 发送请求
        envoyServer.increase(request, responseObserver);
        
        // 等待一下让异步处理完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void testPlaceOrder() {
        System.out.println("\n=== 测试下单请求 ===");
        
        // 创建下单请求
        Envoy.PlaceOrderRequest request = Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(2025060202L)
                .setAccountId(1)
                .setSide(Envoy.Side.BID)
                .setSymbolId(1)
                .setPrice("10")
                .setQuantity("0.5")
                .setType(Envoy.Type.LIMIT)
                .build();

        System.out.println("发送下单请求: " + request);

        // 创建响应观察者
        FakeStreamObserver<Envoy.PlaceOrderResponse> responseObserver = FakeStreamObserver.of(response -> {
            System.out.println("收到下单响应:");
            System.out.println("  状态码: " + response.getCode());
            System.out.println("  消息: " + (response.hasMessage() ? response.getMessage() : "无"));
            if (response.hasData()) {
                Envoy.Order order = response.getData();
                System.out.println("  订单信息:");
                System.out.println("    订单ID: " + order.getId());
            }
        });

        // 发送请求
        envoyServer.placeOrder(request, responseObserver);
        
        // 等待一下让异步处理完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
