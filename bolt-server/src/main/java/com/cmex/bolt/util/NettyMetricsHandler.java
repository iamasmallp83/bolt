package com.cmex.bolt.util;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http.*;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;
import io.prometheus.client.CollectorRegistry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Netty HTTP处理器，用于暴露Prometheus指标
 * 集成到现有的gRPC服务器中，避免额外的HTTP服务器
 */
public class NettyMetricsHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    
    private static final String METRICS_PATH = "/metrics";
    private static final String HEALTH_PATH = "/health";
    private static final String CONTENT_TYPE_PROMETHEUS = "text/plain; version=0.0.4; charset=utf-8";
    private static final String CONTENT_TYPE_JSON = "application/json; charset=utf-8";
    
    // 使用单线程执行器处理指标生成，避免阻塞Netty EventLoop
    private final Executor metricsExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "metrics-generator");
        t.setDaemon(true);
        return t;
    });
    
    private final CollectorRegistry registry;
    
    public NettyMetricsHandler() {
        this(CollectorRegistry.defaultRegistry);
    }
    
    public NettyMetricsHandler(CollectorRegistry registry) {
        this.registry = registry;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String uri = request.uri();
        HttpMethod method = request.method();
        
        // 只处理GET请求
        if (!HttpMethod.GET.equals(method)) {
            sendError(ctx, HttpResponseStatus.METHOD_NOT_ALLOWED, "Method not allowed");
            return;
        }
        
        // 路由处理
        if (METRICS_PATH.equals(uri)) {
            handleMetrics(ctx);
        } else if (HEALTH_PATH.equals(uri)) {
            handleHealth(ctx);
        } else {
            sendError(ctx, HttpResponseStatus.NOT_FOUND, "Not found");
        }
    }
    
    /**
     * 处理/metrics端点
     */
    private void handleMetrics(ChannelHandlerContext ctx) {
        // 异步生成指标，避免阻塞EventLoop
        CompletableFuture.supplyAsync(() -> {
            try {
                return generateMetrics();
            } catch (Exception e) {
                throw new RuntimeException("Failed to generate metrics", e);
            }
        }, metricsExecutor).whenComplete((metrics, throwable) -> {
            if (throwable != null) {
                sendError(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error");
            } else {
                sendMetricsResponse(ctx, metrics);
            }
        });
    }
    
    /**
     * 处理/health端点
     */
    private void handleHealth(ChannelHandlerContext ctx) {
        String healthJson = "{\"status\":\"UP\",\"timestamp\":" + System.currentTimeMillis() + "}";
        sendJsonResponse(ctx, healthJson);
    }
    
    /**
     * 生成Prometheus格式的指标
     */
    private String generateMetrics() {
        StringBuilder sb = new StringBuilder();
        java.util.Enumeration<io.prometheus.client.Collector.MetricFamilySamples> samples = 
            registry.metricFamilySamples();
        
        while (samples.hasMoreElements()) {
            io.prometheus.client.Collector.MetricFamilySamples family = samples.nextElement();
            sb.append(family.toString()).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * 发送指标响应
     */
    private void sendMetricsResponse(ChannelHandlerContext ctx, String metrics) {
        ByteBuf content = Unpooled.copiedBuffer(metrics, CharsetUtil.UTF_8);
        
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            content
        );
        
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE_PROMETHEUS);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
        
        ctx.writeAndFlush(response);
    }
    
    /**
     * 发送JSON响应
     */
    private void sendJsonResponse(ChannelHandlerContext ctx, String json) {
        ByteBuf content = Unpooled.copiedBuffer(json, CharsetUtil.UTF_8);
        
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK,
            content
        );
        
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE_JSON);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-cache");
        
        ctx.writeAndFlush(response);
    }
    
    /**
     * 发送错误响应
     */
    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
        ByteBuf content = Unpooled.copiedBuffer(message, CharsetUtil.UTF_8);
        
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            content
        );
        
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        
        ctx.writeAndFlush(response);
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
    
    /**
     * 关闭处理器，清理资源
     */
    public void shutdown() {
        if (metricsExecutor instanceof java.util.concurrent.ExecutorService) {
            ((java.util.concurrent.ExecutorService) metricsExecutor).shutdown();
        }
    }
}
