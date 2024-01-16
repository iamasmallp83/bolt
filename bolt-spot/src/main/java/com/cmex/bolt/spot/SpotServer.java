package com.cmex.bolt.spot;

import com.cmex.bolt.spot.service.AccountDispatcher;
import com.cmex.bolt.spot.service.OrderDispatcher;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import net.devh.boot.grpc.server.serverfactory.GrpcServerConfigurer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SpotServer {

    public static void main(String[] args) throws IOException, InterruptedException {
        SpringApplication.run(SpotServer.class, args);
    }

    @Bean
    public GrpcServerConfigurer keepAliveServerConfigurer() {
        return serverBuilder -> {
            if (serverBuilder instanceof NettyServerBuilder) {
                ((NettyServerBuilder) serverBuilder)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(5, TimeUnit.SECONDS)
                        .permitKeepAliveWithoutCalls(true);
            }
        };
    }

    @Bean
    public List<AccountDispatcher> accountDispatchers() {
        List<AccountDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new AccountDispatcher(i));
        }
        return dispatchers;
    }

    @Bean
    public List<OrderDispatcher> orderDispatchers() {
        List<OrderDispatcher> dispatchers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dispatchers.add(new OrderDispatcher(i));
        }
        return dispatchers;
    }

}
