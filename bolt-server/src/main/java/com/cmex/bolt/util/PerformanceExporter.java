package com.cmex.bolt.util;

import com.lmax.disruptor.RingBuffer;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 性能导出器
 * 用于监控RingBuffer容量、gRPC方法性能、机器信息等
 * 高级优化版本：使用volatile变量+读写锁，减少原子操作开销
 */
public class PerformanceExporter {
    private final RingBuffer<?> ringBuffer;
    private final double highWatermark;
    private final double criticalWatermark;

    // 统计信息
    private final LongAdder rejectedRequests = new LongAdder();
    private final LongAdder totalRequests = new LongAdder();
    private final AtomicLong maxUsageRate = new AtomicLong(0);
    
    // 使用volatile变量存储状态，减少原子操作开销
    private volatile double cachedUsageRate = 0.0;
    private volatile long cachedCapacity = 0L;
    private volatile long cachedRemaining = 0L;
    private volatile boltResult currentState = boltResult.NORMAL;
    
    // 读写锁，用于保护状态更新
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
    
    // 后台更新线程
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PerformanceExporter-Updater");
        t.setDaemon(true);
        return t;
    });
    
    // 自适应更新频率控制
    private volatile int updateIntervalMs = 100; // 初始100ms
    private static final int MIN_UPDATE_INTERVAL = 50;  // 最小50ms
    private static final int MAX_UPDATE_INTERVAL = 1000; // 最大1秒
    
    // 机器信息相关
    private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    private final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    
    // RingBuffer 背压相关 Prometheus 指标
    private static final Counter TOTAL_REQUESTS = Counter.build()
            .name("bolt_requests_total")
            .help("Total number of requests checked by bolt manager")
            .labelNames("status")
            .register();
    
    private static final Counter REJECTED_REQUESTS = Counter.build()
            .name("bolt_rejected_requests_total")
            .help("Total number of rejected requests")
            .register();
    
    private static final Gauge USAGE_RATE = Gauge.build()
            .name("bolt_ring_buffer_usage_rate")
            .help("Current RingBuffer usage rate (0.0-1.0)")
            .register();
    
    private static final Gauge MAX_USAGE_RATE = Gauge.build()
            .name("bolt_ring_buffer_max_usage_rate")
            .help("Maximum RingBuffer usage rate observed")
            .register();
    
    private static final Gauge REMAINING_CAPACITY = Gauge.build()
            .name("bolt_ring_buffer_remaining_capacity")
            .help("Current remaining capacity in RingBuffer")
            .register();
    
    private static final Gauge TOTAL_CAPACITY = Gauge.build()
            .name("bolt_total_capacity")
            .help("Total capacity of RingBuffer")
            .register();
    
    private static final Gauge CURRENT_STATE = Gauge.build()
            .name("bolt_current_state")
            .help("Current bolt state (0=Normal, 1=HighLoad, 2=Critical)")
            .register();
    
    private static final Summary CHECK_DURATION = Summary.build()
            .name("bolt_check_duration_seconds")
            .help("Time spent checking bolt capacity")
            .register();
    
    // gRPC 方法性能指标
    private static final Counter GRPC_REQUESTS_TOTAL = Counter.build()
            .name("grpc_requests_total")
            .help("Total number of gRPC requests")
            .labelNames("method", "status")
            .register();
    
    private static final Histogram GRPC_REQUEST_DURATION = Histogram.build()
            .name("grpc_request_duration_milliseconds")
            .help("gRPC request duration in milliseconds")
            .labelNames("method")
            .buckets(1.0, 5.0, 10.0, 50.0, 100.0)
            .register();
    
    private static final Summary GRPC_REQUEST_LATENCY = Summary.build()
            .name("grpc_request_latency_milliseconds")
            .help("gRPC request latency in milliseconds")
            .labelNames("method")
            .quantile(0.5, 0.01)
            .quantile(0.9, 0.01)
            .quantile(0.95, 0.01)
            .quantile(0.99, 0.01)
            .register();
    
    // 机器信息指标
    private static final Gauge SYSTEM_CPU_USAGE = Gauge.build()
            .name("system_cpu_usage")
            .help("System CPU usage percentage")
            .register();
    
    private static final Gauge SYSTEM_LOAD_AVERAGE = Gauge.build()
            .name("system_load_average")
            .help("System load average")
            .register();
    
    private static final Gauge JVM_MEMORY_HEAP_USED = Gauge.build()
            .name("jvm_memory_heap_used_bytes")
            .help("JVM heap memory used in bytes")
            .register();
    
    private static final Gauge JVM_MEMORY_HEAP_MAX = Gauge.build()
            .name("jvm_memory_heap_max_bytes")
            .help("JVM heap memory max in bytes")
            .register();
    
    private static final Gauge JVM_MEMORY_NON_HEAP_USED = Gauge.build()
            .name("jvm_memory_non_heap_used_bytes")
            .help("JVM non-heap memory used in bytes")
            .register();
    
    private static final Gauge JVM_MEMORY_NON_HEAP_MAX = Gauge.build()
            .name("jvm_memory_non_heap_max_bytes")
            .help("JVM non-heap memory max in bytes")
            .register();
    
    private static final Gauge JVM_UPTIME = Gauge.build()
            .name("jvm_uptime_seconds")
            .help("JVM uptime in seconds")
            .register();
    
    private static final Gauge JVM_THREADS_COUNT = Gauge.build()
            .name("jvm_threads_count")
            .help("JVM thread count")
            .register();
    
    public PerformanceExporter(RingBuffer<?> ringBuffer) {
        this(ringBuffer, 0.75, 0.9);
    }
    
    public PerformanceExporter(RingBuffer<?> ringBuffer, double highWatermark, double criticalWatermark) {
        this.ringBuffer = ringBuffer;
        this.highWatermark = highWatermark;
        this.criticalWatermark = criticalWatermark;
        
        if (highWatermark >= criticalWatermark) {
            throw new IllegalArgumentException("highWatermark must be less than criticalWatermark");
        }
        
        // 初始化状态
        updateCachedState();
        
        // 启动后台更新线程，使用自适应频率
        scheduleNextUpdate();
    }
    
    /**
     * 调度下一次更新（自适应频率）
     */
    private void scheduleNextUpdate() {
        scheduler.schedule(this::updateCachedState, updateIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 后台线程更新缓存状态（超优化版本：自适应频率+批量更新+Prometheus指标）
     */
    private void updateCachedState() {
        long capacity = ringBuffer.getBufferSize();
        long remaining = ringBuffer.remainingCapacity();
        double usageRate = (double)(capacity - remaining) / capacity;
        
        // 使用写锁保护状态更新
        stateLock.writeLock().lock();
        try {
            cachedCapacity = capacity;
            cachedRemaining = remaining;
            cachedUsageRate = usageRate;
            
            // 预计算状态，避免checkCapacity中的重复计算
            if (usageRate >= criticalWatermark) {
                currentState = boltResult.CRITICAL_REJECT;
            } else if (usageRate >= highWatermark) {
                currentState = boltResult.HIGH_LOAD;
            } else {
                currentState = boltResult.NORMAL;
            }
        } finally {
            stateLock.writeLock().unlock();
        }
        
        // 更新最大使用率
        updateMaxUsageRate(usageRate);
        
        // 更新 Prometheus 指标
        updatePrometheusMetrics(capacity, remaining, usageRate);
        updateMachineMetrics();
        
        // 自适应调整更新频率
        adjustUpdateFrequency(usageRate);
        
        // 调度下一次更新
        scheduleNextUpdate();
    }
    
    /**
     * 更新 Prometheus 指标
     */
    private void updatePrometheusMetrics(long capacity, long remaining, double usageRate) {
        // 更新 Gauge 指标
        USAGE_RATE.set(usageRate);
        REMAINING_CAPACITY.set(remaining);
        TOTAL_CAPACITY.set(capacity);
        MAX_USAGE_RATE.set(Double.longBitsToDouble(maxUsageRate.get()));
        
        // 更新状态指标（0=Normal, 1=HighLoad, 2=Critical）
        int stateValue = switch (currentState) {
            case NORMAL -> 0;
            case HIGH_LOAD -> 1;
            case CRITICAL_REJECT -> 2;
        };
        CURRENT_STATE.set(stateValue);
    }
    
    /**
     * 更新机器信息指标
     */
    private void updateMachineMetrics() {
        // CPU 使用率
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
            SYSTEM_CPU_USAGE.set(sunOsBean.getProcessCpuLoad());
        }
        
        // 系统负载
        double loadAverage = osBean.getSystemLoadAverage();
        if (loadAverage >= 0) {
            SYSTEM_LOAD_AVERAGE.set(loadAverage);
        }
        
        // JVM 内存信息
        JVM_MEMORY_HEAP_USED.set(memoryBean.getHeapMemoryUsage().getUsed());
        JVM_MEMORY_HEAP_MAX.set(memoryBean.getHeapMemoryUsage().getMax());
        JVM_MEMORY_NON_HEAP_USED.set(memoryBean.getNonHeapMemoryUsage().getUsed());
        JVM_MEMORY_NON_HEAP_MAX.set(memoryBean.getNonHeapMemoryUsage().getMax());
        
        // JVM 运行时间
        JVM_UPTIME.set(runtimeBean.getUptime() / 1000.0);
        
        // 线程数
        JVM_THREADS_COUNT.set(ManagementFactory.getThreadMXBean().getThreadCount());
    }
    
    /**
     * 根据使用率自适应调整更新频率
     */
    private void adjustUpdateFrequency(double usageRate) {
        // 根据使用率调整更新频率
        if (usageRate >= criticalWatermark) {
            // 临界状态，更频繁更新
            updateIntervalMs = Math.max(MIN_UPDATE_INTERVAL, updateIntervalMs / 2);
        } else if (usageRate >= highWatermark) {
            // 高负载状态，适度更新
            updateIntervalMs = Math.max(MIN_UPDATE_INTERVAL, (int)(updateIntervalMs * 0.8));
        } else if (usageRate < highWatermark * 0.5) {
            // 低负载状态，减少更新频率
            updateIntervalMs = Math.min(MAX_UPDATE_INTERVAL, (int)(updateIntervalMs * 1.2));
        }
    }
    
    /**
     * 检查当前容量状态（超优化版本：返回是否接受请求+Prometheus指标）
     * @return true表示可以接受请求，false表示拒绝请求
     */
    public boolean checkCapacity() {
        // 使用 Summary 记录检查耗时
        Summary.Timer timer = CHECK_DURATION.startTimer();
        try {
            totalRequests.increment();
            
            // 直接使用预计算的状态，避免任何计算和比较
            boltResult result = currentState;
            
            // 记录请求状态到 Prometheus
            String statusLabel = switch (result) {
                case NORMAL -> "normal";
                case HIGH_LOAD -> "high_load";
                case CRITICAL_REJECT -> "critical_reject";
            };
            TOTAL_REQUESTS.labels(statusLabel).inc();
            
            // 只有在CRITICAL状态时才拒绝请求并更新拒绝计数
            if (result == boltResult.CRITICAL_REJECT) {
                rejectedRequests.increment();
                REJECTED_REQUESTS.inc();
                return false; // 拒绝请求
            }
            
            return true; // 接受请求（包括NORMAL和HIGH_LOAD状态）
        } finally {
            timer.observeDuration();
        }
    }
    
    /**
     * 记录 gRPC 方法调用
     * @param methodName 方法名
     * @param success 是否成功
     * @param duration 耗时（秒）
     */
    public void recordGrpcCall(String methodName, boolean success, double duration) {
        String status = success ? "success" : "error";
        GRPC_REQUESTS_TOTAL.labels(methodName, status).inc();
        // 转换为毫秒
        double durationMs = duration * 1000.0;
        GRPC_REQUEST_DURATION.labels(methodName).observe(durationMs);
        GRPC_REQUEST_LATENCY.labels(methodName).observe(durationMs);
    }
    
    /**
     * 创建 gRPC 方法计时器
     * @param methodName 方法名
     * @return 计时器
     */
    public GrpcTimer createGrpcTimer(String methodName) {
        return new GrpcTimer(methodName, this);
    }
    
    /**
     * 获取当前使用率（超优化版本：直接读取volatile变量）
     * @return 0.0-1.0之间的使用率
     */
    public double getCurrentUsageRate() {
        return cachedUsageRate;
    }
    
    /**
     * 获取当前背压状态（用于监控和日志）
     * @return 当前背压状态
     */
    public boltResult getCurrentState() {
        return currentState;
    }
    
    /**
     * 检查是否处于高负载状态
     * @return true表示处于高负载状态
     */
    public boolean isHighLoad() {
        return currentState == boltResult.HIGH_LOAD;
    }
    
    /**
     * 检查是否处于临界状态
     * @return true表示处于临界状态
     */
    public boolean isCritical() {
        return currentState == boltResult.CRITICAL_REJECT;
    }
    
    /**
     * 更新最大使用率（线程安全）
     */
    private void updateMaxUsageRate(double usageRate) {
        long currentMax = maxUsageRate.get();
        long newMax = Double.doubleToLongBits(usageRate);
        while (usageRate > Double.longBitsToDouble(currentMax)) {
            if (maxUsageRate.compareAndSet(currentMax, newMax)) {
                break;
            }
            currentMax = maxUsageRate.get();
        }
    }
    
    /**
     * 获取统计信息（超优化版本：使用读锁保护，批量读取）
     * @return 背压统计信息
     */
    public boltStats getStats() {
        stateLock.readLock().lock();
        try {
            double currentUsageRate = cachedUsageRate;
            long capacity = cachedCapacity;
            long remaining = cachedRemaining;
            double maxUsage = Double.longBitsToDouble(maxUsageRate.get());
            
            return new boltStats(
                currentUsageRate,
                maxUsage,
                rejectedRequests.sum(),
                totalRequests.sum(),
                capacity,
                remaining,
                highWatermark,
                criticalWatermark
            );
        } finally {
            stateLock.readLock().unlock();
        }
    }
    
    /**
     * 格式化统计信息用于日志输出
     */
    public String formatStats() {
        boltStats stats = getStats();
        return String.format("[%s] Usage: %.2f%% (Max: %.2f%%), Rejected: %d/%d (%.2f%%), Remaining: %d/%d",
            stats.currentUsageRate() * 100,
            stats.maxUsageRate() * 100,
            stats.rejectedRequests(),
            stats.totalRequests(),
            stats.totalRequests() > 0 ? (double)stats.rejectedRequests() / stats.totalRequests() * 100 : 0.0,
            stats.remaining(),
            stats.capacity()
        );
    }
    
    /**
     * 获取 Prometheus 指标格式的字符串
     * @return Prometheus 格式的指标数据
     */
    public String getPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();
        var samples = io.prometheus.client.CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (samples.hasMoreElements()) {
            sb.append(samples.nextElement().toString()).append("\n");
        }
        return sb.toString();
    }
    
    /**
     * 关闭性能导出器，清理资源
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * 背压检查结果
     */
    public enum boltResult {
        NORMAL,           // 正常，可以接受请求
        HIGH_LOAD,        // 高负载，可以接受但需要警告
        CRITICAL_REJECT   // 临界状态，拒绝请求
    }
    
    /**
     * 背压统计信息
     */
    public record boltStats(
        double currentUsageRate,
        double maxUsageRate,
        long rejectedRequests,
        long totalRequests,
        long capacity,
        long remaining,
        double highWatermark,
        double criticalWatermark
    ) {
        public double getRejectionRate() {
            return totalRequests > 0 ? (double)rejectedRequests / totalRequests : 0.0;
        }
        
        public boolean isHighLoad() {
            return currentUsageRate >= highWatermark;
        }
        
        public boolean isCritical() {
            return currentUsageRate >= criticalWatermark;
        }
    }
    
    /**
     * gRPC 方法计时器
     */
    public static class GrpcTimer implements AutoCloseable {
        private final String methodName;
        private final PerformanceExporter exporter;
        private final long startTime;
        private boolean closed = false;
        
        public GrpcTimer(String methodName, PerformanceExporter exporter) {
            this.methodName = methodName;
            this.exporter = exporter;
            this.startTime = System.nanoTime();
        }
        
        public void recordSuccess() {
            if (!closed) {
                double duration = (System.nanoTime() - startTime) / 1_000_000_000.0;
                exporter.recordGrpcCall(methodName, true, duration);
                closed = true;
            }
        }
        
        public void recordError() {
            if (!closed) {
                double duration = (System.nanoTime() - startTime) / 1_000_000_000.0;
                exporter.recordGrpcCall(methodName, false, duration);
                closed = true;
            }
        }
        
        @Override
        public void close() {
            if (!closed) {
                recordSuccess(); // 默认记录为成功
            }
        }
    }
}
