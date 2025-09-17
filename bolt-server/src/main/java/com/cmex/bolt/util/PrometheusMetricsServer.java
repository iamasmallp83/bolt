package com.cmex.bolt.util;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.CollectorRegistry;
import lombok.Getter;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Prometheus 指标服务器
 * 用于暴露 BackpressureManager 的监控指标
 */
public class PrometheusMetricsServer {
    private final HTTPServer httpServer;
    /**
     * -- GETTER --
     *  获取服务器端口
     */
    @Getter
    private final int port;
    
    public PrometheusMetricsServer(int port) throws IOException {
        this.port = port;
        this.httpServer = new HTTPServer(new InetSocketAddress(port), CollectorRegistry.defaultRegistry);
    }
    
    public PrometheusMetricsServer() throws IOException {
        this(9091); // 默认端口 8080
    }

    /**
     * 关闭指标服务器
     */
    public void shutdown() {
        if (httpServer != null) {
            httpServer.stop();
        }
    }
    
    /**
     * 检查服务器是否正在运行
     */
    public boolean isRunning() {
        return httpServer != null;
    }
}
