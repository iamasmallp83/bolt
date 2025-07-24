package com.cmex.bolt.util;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import lombok.Getter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 通用对象池 - 基于Guava优化的高性能对象池实现
 * 
 * 特性：
 * 1. 线程安全的对象借用和归还
 * 2. 可配置的池大小和对象验证
 * 3. 详细的统计信息和监控
 * 4. 支持对象重置和清理逻辑
 * 5. 内存泄漏保护机制
 * 
 * @param <T> 池中对象的类型
 */
public class GenericObjectPool<T> {

    private final ArrayBlockingQueue<T> pool;
    private final Supplier<T> objectFactory;
    private final Consumer<T> resetFunction;
    private final Predicate<T> validator;
    private final int maxPoolSize;
    private final int maxObjectSize;

    // 统计信息 - 线程安全的原子计数器
    private final AtomicLong borrowCount = new AtomicLong(0);
    private final AtomicLong returnCount = new AtomicLong(0);
    private final AtomicLong createCount = new AtomicLong(0);
    private final AtomicLong discardCount = new AtomicLong(0);
    private final AtomicLong validationFailCount = new AtomicLong(0);

    /**
     * 创建对象池
     * 
     * @param poolSize 池的大小
     * @param objectFactory 对象创建工厂
     * @param resetFunction 对象重置函数（归还时调用）
     * @param validator 对象验证器（验证对象是否可重用）
     * @param maxObjectSize 对象的最大大小限制（防止内存泄漏）
     */
    public GenericObjectPool(int poolSize, 
                           Supplier<T> objectFactory,
                           Consumer<T> resetFunction,
                           Predicate<T> validator,
                           int maxObjectSize) {
        this.maxPoolSize = poolSize;
        this.pool = new ArrayBlockingQueue<>(poolSize);
        this.objectFactory = objectFactory;
        this.resetFunction = resetFunction;
        this.validator = validator;
        this.maxObjectSize = maxObjectSize;
        
        preWarmPool();
    }

    /**
     * 简化构造器 - 只需要基本参数
     */
    public GenericObjectPool(int poolSize, Supplier<T> objectFactory, Consumer<T> resetFunction) {
        this(poolSize, objectFactory, resetFunction, obj -> true, Integer.MAX_VALUE);
    }

    /**
     * 预热对象池 - 创建初始对象
     */
    private void preWarmPool() {
        for (int i = 0; i < maxPoolSize; i++) {
            T obj = objectFactory.get();
            createCount.incrementAndGet();
            pool.offer(obj);
        }
    }

    /**
     * 从池中借用对象
     * 
     * @return 可用的对象，如果池为空则创建新对象
     */
    public T borrow() {
        borrowCount.incrementAndGet();
        
        T obj = pool.poll();
        if (obj == null) {
            // 池为空，创建新对象
            obj = objectFactory.get();
            createCount.incrementAndGet();
        }
        
        return obj;
    }

    /**
     * 归还对象到池中
     * 
     * @param obj 要归还的对象
     * @return true如果成功归还，false如果对象被丢弃
     */
    public boolean returnObject(T obj) {
        if (obj == null) {
            return false;
        }

        returnCount.incrementAndGet();

        try {
            // 1. 重置对象状态
            if (resetFunction != null) {
                resetFunction.accept(obj);
            }

            // 2. 验证对象是否可重用
            if (!validator.test(obj)) {
                validationFailCount.incrementAndGet();
                discardCount.incrementAndGet();
                return false;
            }

            // 3. 检查对象大小（防止内存泄漏）
            if (!isObjectSizeAcceptable(obj)) {
                discardCount.incrementAndGet();
                return false;
            }

            // 4. 尝试归还到池中
            boolean offered = pool.offer(obj);
            if (!offered) {
                // 池已满，丢弃对象
                discardCount.incrementAndGet();
            }
            return offered;

        } catch (Exception e) {
            // 重置或验证过程中出现异常，丢弃对象
            discardCount.incrementAndGet();
            return false;
        }
    }

    /**
     * 检查对象大小是否可接受
     * 子类可以重写此方法实现具体的大小检查逻辑
     */
    protected boolean isObjectSizeAcceptable(T obj) {
        // 默认实现：对于集合类型检查大小
        if (obj instanceof java.util.Collection) {
            return ((java.util.Collection<?>) obj).size() <= maxObjectSize;
        }
        return true;
    }

    /**
     * 获取池的统计信息
     */
    public PoolStatistics getStatistics() {
        long totalRequests = borrowCount.get();
        long poolHits = totalRequests - createCount.get();
        double hitRate = totalRequests > 0 ? (double) poolHits / totalRequests * 100 : 0;

        return new PoolStatistics(
            borrowCount.get(),
            returnCount.get(),
            createCount.get(),
            discardCount.get(),
            validationFailCount.get(),
            hitRate,
            pool.size(),
            maxPoolSize
        );
    }

    /**
     * 清空池中所有对象
     */
    public void clear() {
        pool.clear();
    }

    /**
     * 获取当前池大小
     */
    public int getCurrentPoolSize() {
        return pool.size();
    }

    /**
     * 池统计信息记录
     */
    @Getter
    public static class PoolStatistics {
        private final long borrowCount;        // 借用次数
        private final long returnCount;        // 归还次数
        private final long createCount;        // 创建次数
        private final long discardCount;       // 丢弃次数
        private final long validationFailCount; // 验证失败次数
        private final double hitRate;          // 命中率%
        private final int currentPoolSize;     // 当前池大小
        private final int maxPoolSize;         // 最大池大小

        public PoolStatistics(long borrowCount, long returnCount, long createCount, 
                            long discardCount, long validationFailCount, double hitRate,
                            int currentPoolSize, int maxPoolSize) {
            this.borrowCount = borrowCount;
            this.returnCount = returnCount;
            this.createCount = createCount;
            this.discardCount = discardCount;
            this.validationFailCount = validationFailCount;
            this.hitRate = hitRate;
            this.currentPoolSize = currentPoolSize;
            this.maxPoolSize = maxPoolSize;
        }

        @Override
        public String toString() {
            return String.format(
                "PoolStats{borrows=%d, returns=%d, creates=%d, discards=%d, " +
                "validationFails=%d, hitRate=%.1f%%, size=%d/%d}",
                borrowCount, returnCount, createCount, discardCount,
                validationFailCount, hitRate, currentPoolSize, maxPoolSize
            );
        }

        /**
         * 获取详细的性能报告
         */
        public String getDetailedReport() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== 对象池性能报告 ===\n");
            sb.append(String.format("借用次数: %,d\n", borrowCount));
            sb.append(String.format("归还次数: %,d\n", returnCount));
            sb.append(String.format("创建次数: %,d\n", createCount));
            sb.append(String.format("丢弃次数: %,d\n", discardCount));
            sb.append(String.format("验证失败: %,d\n", validationFailCount));
            sb.append(String.format("命中率: %.2f%%\n", hitRate));
            sb.append(String.format("当前池大小: %d/%d\n", currentPoolSize, maxPoolSize));
            sb.append(String.format("内存效率: 避免了 %,d 次对象创建\n", borrowCount - createCount));
            return sb.toString();
        }
    }
} 