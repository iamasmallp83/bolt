package com.cmex.bolt.util;

import com.lmax.disruptor.EventFactory;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 共享ByteBuffer池实现
 * 
 * 核心思想：
 * 1. 预分配一大块连续内存
 * 2. 将内存均匀分成N份，每份对应一个Message
 * 3. 通过切片(slice)为每个Message分配独立的ByteBuffer视图
 * 4. 避免频繁的内存分配，提高性能和减少GC压力
 */
public class SharedByteBufferPool {

    /**
     * 共享内存池配置
     */
    public static class PoolConfig {
        public final int totalPoolSize;           // 总池大小(字节)
        public final int sliceSize;               // 每个切片大小(字节)
        public final int maxSlices;               // 最大切片数量
        public final boolean useDirect;           // 是否使用直接内存
        public final String poolName;             // 池名称
        
        private PoolConfig(Builder builder) {
            this.totalPoolSize = builder.totalPoolSize;
            this.sliceSize = builder.sliceSize;
            this.maxSlices = builder.maxSlices;
            this.useDirect = builder.useDirect;
            this.poolName = builder.poolName;
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public static class Builder {
            private int totalPoolSize = 512 * 1024 * 1024; // 512MB
            private int sliceSize = 512;                    // 512字节每片
            private boolean useDirect = true;               // 使用直接内存
            private String poolName = "DefaultPool";
            private int maxSlices;
            
            public Builder totalPoolSize(int totalPoolSize) {
                this.totalPoolSize = totalPoolSize;
                return this;
            }
            
            public Builder sliceSize(int sliceSize) {
                this.sliceSize = sliceSize;
                return this;
            }
            
            public Builder useDirect(boolean useDirect) {
                this.useDirect = useDirect;
                return this;
            }
            
            public Builder poolName(String poolName) {
                this.poolName = poolName;
                return this;
            }
            
            public PoolConfig build() {
                this.maxSlices = totalPoolSize / sliceSize;
                return new PoolConfig(this);
            }
        }
    }

    /**
     * 内存池实现
     */
    public static class SharedMemoryPool {
        private final PoolConfig config;
        private final ByteBuffer sharedMemory;
        private final AtomicInteger nextSliceIndex;
        private final AtomicLong totalAllocated;
        private final AtomicLong totalReleased;
        
        public SharedMemoryPool(PoolConfig config) {
            this.config = config;
            this.nextSliceIndex = new AtomicInteger(0);
            this.totalAllocated = new AtomicLong(0);
            this.totalReleased = new AtomicLong(0);
            
            // 分配大块共享内存
            if (config.useDirect) {
                this.sharedMemory = ByteBuffer.allocateDirect(config.totalPoolSize);
            } else {
                this.sharedMemory = ByteBuffer.allocate(config.totalPoolSize);
            }
            
            System.out.printf("🚀 SharedMemoryPool[%s] 初始化完成:\n", config.poolName);
            System.out.printf("   📦 总内存: %.2f MB (%s)\n", 
                config.totalPoolSize / 1024.0 / 1024.0,
                config.useDirect ? "直接内存" : "堆内存");
            System.out.printf("   🔪 切片大小: %d 字节\n", config.sliceSize);
            System.out.printf("   📊 最大切片数: %,d\n", config.maxSlices);
        }
        
        /**
         * 分配下一个ByteBuffer切片
         */
        public ByteBuffer allocateSlice() {
            int sliceIndex = nextSliceIndex.getAndIncrement();
            
            if (sliceIndex >= config.maxSlices) {
                throw new RuntimeException(String.format(
                    "SharedMemoryPool[%s] 耗尽! 请求第 %d 个切片，但最大只有 %d 个",
                    config.poolName, sliceIndex + 1, config.maxSlices));
            }
            
            // 计算切片在共享内存中的偏移量
            int offset = sliceIndex * config.sliceSize;
            
            // 创建切片视图
            ByteBuffer slice = createSlice(offset, config.sliceSize);
            totalAllocated.incrementAndGet();
            
            return slice;
        }
        
        /**
         * 创建内存切片
         */
        private ByteBuffer createSlice(int offset, int size) {
            // 同步访问共享ByteBuffer
            synchronized (sharedMemory) {
                // 设置position和limit来定义切片范围
                sharedMemory.position(offset);
                sharedMemory.limit(offset + size);
                
                // 创建切片视图
                ByteBuffer slice = sharedMemory.slice();
                slice.order(sharedMemory.order()); // 保持字节序一致
                
                // 重置共享缓冲区状态
                sharedMemory.clear();
                
                return slice;
            }
        }
        
        /**
         * 释放切片（在当前实现中主要用于统计）
         */
        public void releaseSlice(ByteBuffer slice) {
            if (slice != null) {
                totalReleased.incrementAndGet();
                // 在基于切片的实现中，不需要实际释放内存
                // 但可以在这里添加切片重用逻辑
            }
        }
        
        /**
         * 重置池（清空所有分配）
         */
        public void reset() {
            nextSliceIndex.set(0);
            totalAllocated.set(0);
            totalReleased.set(0);
            System.out.printf("🔄 SharedMemoryPool[%s] 已重置\n", config.poolName);
        }
        
        /**
         * 获取池统计信息
         */
        public PoolStats getStats() {
            int currentIndex = nextSliceIndex.get();
            double usagePercent = (double) currentIndex / config.maxSlices * 100;
            long allocatedBytes = (long) currentIndex * config.sliceSize;
            
            return new PoolStats(
                config.poolName,
                currentIndex,
                config.maxSlices,
                usagePercent,
                allocatedBytes,
                config.totalPoolSize,
                totalAllocated.get(),
                totalReleased.get()
            );
        }
        
        /**
         * 检查池是否健康
         */
        public boolean isHealthy() {
            double usagePercent = (double) nextSliceIndex.get() / config.maxSlices * 100;
            return usagePercent < 95.0; // 使用率低于95%认为健康
        }
        
        /**
         * 获取可用切片数
         */
        public int getAvailableSlices() {
            return config.maxSlices - nextSliceIndex.get();
        }
    }

    /**
     * 针对Message优化的EventFactory
     */
    public static class SharedBufferMessageFactory implements EventFactory<ByteBuffer> {
        @Getter
        private final SharedMemoryPool memoryPool;
        private final AtomicLong factoryCallCount = new AtomicLong(0);
        
        public SharedBufferMessageFactory(PoolConfig config) {
            this.memoryPool = new SharedMemoryPool(config);
        }
        
        public SharedBufferMessageFactory(SharedMemoryPool memoryPool) {
            this.memoryPool = memoryPool;
        }
        
        @Override
        public ByteBuffer newInstance() {
            try {
                // 创建Message对象
                // 为Message分配共享内存切片
                ByteBuffer slice = memoryPool.allocateSlice();
                factoryCallCount.incrementAndGet();
                return slice;

            } catch (Exception e) {
                throw e;
            }
        }
        
        /**
         * 获取工厂统计信息
         */
        public String getFactoryStats() {
            PoolStats poolStats = memoryPool.getStats();
            return String.format("Factory[%s]: 调用=%d次, %s", 
                poolStats.poolName(), factoryCallCount.get(), poolStats);
        }
        
        /**
         * 重置工厂
         */
        public void reset() {
            memoryPool.reset();
            factoryCallCount.set(0);
        }
    }

    /**
     * 预配置的工厂实例
     */
    public static class PreConfiguredFactories {
        
        /**
         * RingBuffer专用工厂 - 针对512K RingBuffer优化
         */
        public static SharedBufferMessageFactory createRingBufferFactory(String name, int ringBufferSize) {
            // 为RingBuffer预分配足够的内存
            int sliceSize = 512; // 每个Message 512字节
            int totalSize = ringBufferSize * sliceSize * 2; // 预留100%冗余
            
            PoolConfig config = PoolConfig.builder()
                .poolName(name)
                .totalPoolSize(totalSize)
                .sliceSize(sliceSize)
                .useDirect(true) // 使用直接内存减少GC压力
                .build();
                
            return new SharedBufferMessageFactory(config);
        }
        
        /**
         * Account RingBuffer工厂
         */
        public static SharedBufferMessageFactory createAccountFactory() {
            return createRingBufferFactory("Account-RingBuffer", 1024 * 512);
        }
        
        /**
         * Match RingBuffer工厂
         */
        public static SharedBufferMessageFactory createMatchFactory() {
            return createRingBufferFactory("Match-RingBuffer", 1024 * 256);
        }
        
        /**
         * Response RingBuffer工厂
         */
        public static SharedBufferMessageFactory createResponseFactory() {
            return createRingBufferFactory("Response-RingBuffer", 1024 * 256);
        }
        
        /**
         * 高性能组合工厂 - 一个大池支持所有RingBuffer
         */
        public static SharedBufferMessageFactory createUnifiedFactory() {
            // 计算总需求
            int accountSize = 1024 * 512;
            int matchSize = 1024 * 256;
            int responseSize = 1024 * 256;
            int totalMessages = accountSize + matchSize + responseSize;
            
            int sliceSize = 512;
            int totalSize = totalMessages * sliceSize * 2; // 100%冗余
            
            PoolConfig config = PoolConfig.builder()
                .poolName("Unified-RingBuffer")
                .totalPoolSize(totalSize)
                .sliceSize(sliceSize)
                .useDirect(true)
                .build();
                
            return new SharedBufferMessageFactory(config);
        }
    }

    /**
     * 池统计信息
     */
    public record PoolStats(
        String poolName,
        int usedSlices,
        int maxSlices,
        double usagePercent,
        long allocatedBytes,
        long totalBytes,
        long totalAllocations,
        long totalReleases
    ) {
        @Override
        public String toString() {
            return String.format(
                "Pool[%s]: %d/%d切片 (%.1f%%), %.2fMB/%.2fMB, alloc=%d, release=%d",
                poolName, usedSlices, maxSlices, usagePercent,
                allocatedBytes / 1024.0 / 1024.0,
                totalBytes / 1024.0 / 1024.0,
                totalAllocations, totalReleases
            );
        }
        
        public boolean isHealthy() {
            return usagePercent < 95.0;
        }
        
        public double getFragmentation() {
            return totalReleases > 0 ? (double) totalReleases / totalAllocations : 0.0;
        }
    }

    /**
     * 内存池监控器
     */
    public static class PoolMonitor {
        private final SharedMemoryPool pool;
        private final long reportIntervalMs;
        private volatile boolean monitoring = false;
        
        public PoolMonitor(SharedMemoryPool pool, long reportIntervalMs) {
            this.pool = pool;
            this.reportIntervalMs = reportIntervalMs;
        }
        
        public void startMonitoring() {
            if (monitoring) return;
            
            monitoring = true;
            Thread monitorThread = new Thread(() -> {
                while (monitoring) {
                    try {
                        Thread.sleep(reportIntervalMs);
                        PoolStats stats = pool.getStats();
                        
                        if (stats.usagePercent() > 80) {
                            System.out.printf("⚠️ 池使用率警告: %s\n", stats);
                        }
                        
                        if (!pool.isHealthy()) {
                            System.out.printf("❌ 池健康异常: %s\n", stats);
                        }
                        
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }, "PoolMonitor-" + pool.config.poolName);
            
            monitorThread.setDaemon(true);
            monitorThread.start();
        }
        
        public void stopMonitoring() {
            monitoring = false;
        }
    }
}