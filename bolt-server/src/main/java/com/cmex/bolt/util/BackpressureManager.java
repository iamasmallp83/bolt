package com.cmex.bolt.util;

import com.lmax.disruptor.RingBuffer;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * RingBuffer背压管理器
 * 用于监控RingBuffer容量并提供背压控制
 * 高级优化版本：使用volatile变量+读写锁，减少原子操作开销
 */
public class BackpressureManager {
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
    private volatile BackpressureResult currentState = BackpressureResult.NORMAL;
    
    // 读写锁，用于保护状态更新
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();
    
    // 后台更新线程
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "BackpressureManager-Updater");
        t.setDaemon(true);
        return t;
    });
    
    // 自适应更新频率控制
    private volatile int updateIntervalMs = 100; // 初始100ms
    private static final int MIN_UPDATE_INTERVAL = 50;  // 最小50ms
    private static final int MAX_UPDATE_INTERVAL = 1000; // 最大1秒
    
    // Prometheus 指标定义
    private static final Counter TOTAL_REQUESTS = Counter.build()
            .name("backpressure_requests_total")
            .help("Total number of requests checked by backpressure manager")
            .labelNames("status")
            .register();
    
    private static final Counter REJECTED_REQUESTS = Counter.build()
            .name("backpressure_rejected_requests_total")
            .help("Total number of rejected requests")
            .register();
    
    private static final Gauge USAGE_RATE = Gauge.build()
            .name("backpressure_usage_rate")
            .help("Current RingBuffer usage rate (0.0-1.0)")
            .register();
    
    private static final Gauge MAX_USAGE_RATE = Gauge.build()
            .name("backpressure_max_usage_rate")
            .help("Maximum RingBuffer usage rate observed")
            .register();
    
    private static final Gauge REMAINING_CAPACITY = Gauge.build()
            .name("backpressure_remaining_capacity")
            .help("Current remaining capacity in RingBuffer")
            .register();
    
    private static final Gauge TOTAL_CAPACITY = Gauge.build()
            .name("backpressure_total_capacity")
            .help("Total capacity of RingBuffer")
            .register();
    
    private static final Gauge CURRENT_STATE = Gauge.build()
            .name("backpressure_current_state")
            .help("Current backpressure state (0=Normal, 1=HighLoad, 2=Critical)")
            .register();
    
    private static final Summary CHECK_DURATION = Summary.build()
            .name("backpressure_check_duration_seconds")
            .help("Time spent checking backpressure capacity")
            .register();
    
    public BackpressureManager(RingBuffer<?> ringBuffer) {
        this(ringBuffer, 0.75, 0.9);
    }
    
    public BackpressureManager(RingBuffer<?> ringBuffer, double highWatermark, double criticalWatermark) {
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
                currentState = BackpressureResult.CRITICAL_REJECT;
            } else if (usageRate >= highWatermark) {
                currentState = BackpressureResult.HIGH_LOAD;
            } else {
                currentState = BackpressureResult.NORMAL;
            }
        } finally {
            stateLock.writeLock().unlock();
        }
        
        // 更新最大使用率
        updateMaxUsageRate(usageRate);
        
        // 更新 Prometheus 指标
        updatePrometheusMetrics(capacity, remaining, usageRate);
        
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
            BackpressureResult result = currentState;
            
            // 记录请求状态到 Prometheus
            String statusLabel = switch (result) {
                case NORMAL -> "normal";
                case HIGH_LOAD -> "high_load";
                case CRITICAL_REJECT -> "critical_reject";
            };
            TOTAL_REQUESTS.labels(statusLabel).inc();
            
            // 只有在CRITICAL状态时才拒绝请求并更新拒绝计数
            if (result == BackpressureResult.CRITICAL_REJECT) {
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
    public BackpressureResult getCurrentState() {
        return currentState;
    }
    
    /**
     * 检查是否处于高负载状态
     * @return true表示处于高负载状态
     */
    public boolean isHighLoad() {
        return currentState == BackpressureResult.HIGH_LOAD;
    }
    
    /**
     * 检查是否处于临界状态
     * @return true表示处于临界状态
     */
    public boolean isCritical() {
        return currentState == BackpressureResult.CRITICAL_REJECT;
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
    public BackpressureStats getStats() {
        stateLock.readLock().lock();
        try {
            double currentUsageRate = cachedUsageRate;
            long capacity = cachedCapacity;
            long remaining = cachedRemaining;
            double maxUsage = Double.longBitsToDouble(maxUsageRate.get());
            
            return new BackpressureStats(
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
        BackpressureStats stats = getStats();
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
     * 关闭背压管理器，清理资源
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
    public enum BackpressureResult {
        NORMAL,           // 正常，可以接受请求
        HIGH_LOAD,        // 高负载，可以接受但需要警告
        CRITICAL_REJECT   // 临界状态，拒绝请求
    }
    
    /**
     * 背压统计信息
     */
    public record BackpressureStats(
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
} 