package com.cmex.bolt.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.cmex.bolt.Envoy;
import com.cmex.bolt.EnvoyServerGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 简单的gRPC客户端测试类
 * 使用gRPC客户端连接到远程EnvoyServer进行测试
 */
public class GrpcClientTest {

    private static ManagedChannel channel;
    private static EnvoyServerGrpc.EnvoyServerStub asyncStub;

    @BeforeAll
    static void setUp() {
        // 创建gRPC通道连接到EnvoyServer
        channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext() // 使用明文传输，生产环境应该使用TLS
                .build();
        
        // 创建异步stub
        asyncStub = EnvoyServerGrpc.newStub(channel);
        
        System.out.println("gRPC客户端已连接到EnvoyServer (localhost:9090)");
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            System.out.println("gRPC客户端连接已关闭");
        }
    }

    @Test
    void testIncreaseBalance() {
        System.out.println("\n=== 测试增加余额请求 ===");
        
        // 创建增加余额请求
        Envoy.IncreaseRequest request1 = Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1L)
                .setAccountId(1)
                .setAmount("100")
                .setCurrencyId(1)
                .build();

        System.out.println("发送增加余额请求: " + request1);

        // 创建增加余额请求
        Envoy.IncreaseRequest request2 = Envoy.IncreaseRequest.newBuilder()
                .setRequestId(1L)
                .setAccountId(2)
                .setAmount("100")
                .setCurrencyId(2)
                .build();

        System.out.println("发送增加余额请求: " + request2);

        // 使用CountDownLatch等待异步响应
        CountDownLatch latch = new CountDownLatch(2);
        
        // 创建响应观察者
        StreamObserver<Envoy.IncreaseResponse> responseObserver = new StreamObserver<Envoy.IncreaseResponse>() {
            @Override
            public void onNext(Envoy.IncreaseResponse response) {
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
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("增加余额请求失败: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("增加余额请求完成");
            }
        };

        // 发送请求
        asyncStub.increase(request1, responseObserver);
        asyncStub.increase(request2, responseObserver);


        // 等待响应
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("等待响应被中断");
        }
    }

    @Test
    void testPlaceOrder() {
        System.out.println("\n=== 测试下单请求 ===");
        
        // 创建下单请求
        Envoy.PlaceOrderRequest request1 = Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(2025060202L)
                .setAccountId(1)
                .setSide(Envoy.Side.BID)
                .setSymbolId(1)
                .setPrice("10")
                .setQuantity("1")
                .setType(Envoy.Type.LIMIT)
                .build();

        System.out.println("发送下单请求: " + request1);

        Envoy.PlaceOrderRequest request2 = Envoy.PlaceOrderRequest.newBuilder()
                .setRequestId(2025060202L)
                .setAccountId(2)
                .setSide(Envoy.Side.ASK)
                .setSymbolId(1)
                .setPrice("10")
                .setQuantity("0.5")
                .setType(Envoy.Type.LIMIT)
                .build();

        System.out.println("发送下单请求: " + request2);

        // 使用CountDownLatch等待异步响应
        CountDownLatch latch = new CountDownLatch(2);
        
        // 创建响应观察者
        StreamObserver<Envoy.PlaceOrderResponse> responseObserver = new StreamObserver<Envoy.PlaceOrderResponse>() {
            @Override
            public void onNext(Envoy.PlaceOrderResponse response) {
                System.out.println("收到下单响应:");
                System.out.println("  状态码: " + response.getCode());
                System.out.println("  消息: " + (response.hasMessage() ? response.getMessage() : "无"));
                if (response.hasData()) {
                    Envoy.Order order = response.getData();
                    System.out.println("  订单信息:");
                    System.out.println("    订单ID: " + order.getId());
                }
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("下单请求失败: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("下单请求完成");
            }
        };

        // 发送请求
        asyncStub.placeOrder(request1, responseObserver);
        asyncStub.placeOrder(request2, responseObserver);

        // 等待响应
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("等待响应被中断");
        }
    }
}
