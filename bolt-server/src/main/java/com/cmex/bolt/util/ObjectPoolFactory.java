package com.cmex.bolt.util;

import com.google.common.base.Supplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对象池工厂 - 提供常用类型对象池的便捷创建方法
 * 
 * 基于GenericObjectPool，为常见的数据结构提供预配置的对象池
 * 减少样板代码，统一池配置策略
 */
public class ObjectPoolFactory {

    // 默认配置常量
    private static final int DEFAULT_POOL_SIZE = 10;
    private static final int DEFAULT_INITIAL_CAPACITY = 50;
    private static final int DEFAULT_MAX_SIZE = 1000;

    /**
     * 创建ArrayList对象池
     * 适用于需要频繁创建和销毁ArrayList的场景
     * 
     * @param poolSize 池大小
     * @param initialCapacity ArrayList初始容量
     * @param maxSize 列表最大大小限制（防止内存泄漏）
     * @return ArrayList对象池
     */
    public static <T> GenericObjectPool<List<T>> createArrayListPool(
            int poolSize, int initialCapacity, int maxSize) {
        
        return new GenericObjectPool<>(
            poolSize,
            () -> new ArrayList<>(initialCapacity),      // 创建工厂
            List::clear,                                 // 重置函数
            list -> list instanceof ArrayList && list.size() <= maxSize,  // 验证器
            maxSize                                      // 最大大小
        );
    }

    /**
     * 创建ArrayList对象池 - 使用默认配置
     */
    public static <T> GenericObjectPool<List<T>> createArrayListPool() {
        return createArrayListPool(DEFAULT_POOL_SIZE, DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_SIZE);
    }

    /**
     * 创建HashMap对象池
     * 适用于需要频繁创建和销毁Map的场景
     * 
     * @param poolSize 池大小
     * @param initialCapacity HashMap初始容量
     * @param maxSize Map最大大小限制
     * @return HashMap对象池
     */
    public static <K, V> GenericObjectPool<Map<K, V>> createHashMapPool(
            int poolSize, int initialCapacity, int maxSize) {
        
        return new GenericObjectPool<>(
            poolSize,
            () -> new HashMap<>(initialCapacity),       // 创建工厂
            Map::clear,                                 // 重置函数
            map -> map instanceof HashMap && map.size() <= maxSize,  // 验证器
            maxSize                                     // 最大大小
        );
    }

    /**
     * 创建HashMap对象池 - 使用默认配置
     */
    public static <K, V> GenericObjectPool<Map<K, V>> createHashMapPool() {
        return createHashMapPool(DEFAULT_POOL_SIZE, 16, DEFAULT_MAX_SIZE);
    }

    /**
     * 创建StringBuilder对象池
     * 适用于字符串构建场景
     * 
     * @param poolSize 池大小
     * @param initialCapacity StringBuilder初始容量
     * @param maxLength 最大长度限制
     * @return StringBuilder对象池
     */
    public static GenericObjectPool<StringBuilder> createStringBuilderPool(
            int poolSize, int initialCapacity, int maxLength) {
        
        return new GenericObjectPool<>(
            poolSize,
            () -> new StringBuilder(initialCapacity),   // 创建工厂
            sb -> sb.setLength(0),                      // 重置函数：清空内容
            sb -> sb.capacity() <= maxLength * 2,      // 验证器：容量不能过大
            maxLength                                   // 最大大小
        );
    }

    /**
     * 创建StringBuilder对象池 - 使用默认配置
     */
    public static GenericObjectPool<StringBuilder> createStringBuilderPool() {
        return createStringBuilderPool(DEFAULT_POOL_SIZE, 256, 4096);
    }

    /**
     * 创建自定义对象池 - 通用工厂方法
     * 
     * @param poolSize 池大小
     * @param factory 对象创建工厂
     * @param resetFunction 对象重置函数
     * @param <T> 对象类型
     * @return 对象池
     */
    public static <T> GenericObjectPool<T> createCustomPool(
            int poolSize, 
            Supplier<T> factory,
            java.util.function.Consumer<T> resetFunction) {
        
        return new GenericObjectPool<>(poolSize, factory, resetFunction);
    }

    /**
     * 预配置的对象池实例 - 单例模式
     * 为常用场景提供现成的对象池实例
     */
    public static class CommonPools {
        // ArrayList池 - 用于一般列表操作
        public static final GenericObjectPool<List<Object>> ARRAY_LIST_POOL = 
            createArrayListPool();

        // HashMap池 - 用于一般Map操作  
        public static final GenericObjectPool<Map<Object, Object>> HASH_MAP_POOL = 
            createHashMapPool();

        // StringBuilder池 - 用于字符串构建
        public static final GenericObjectPool<StringBuilder> STRING_BUILDER_POOL = 
            createStringBuilderPool();

        // 私有构造器，防止实例化
        private CommonPools() {}
    }

    /**
     * 对象池使用示例和最佳实践
     */
    public static class PoolUsageExample {
        
        /**
         * 演示如何正确使用对象池
         */
        public static void demonstrateUsage() {
            // 创建专用的ArrayList池
            GenericObjectPool<List<String>> stringListPool = 
                ObjectPoolFactory.<String>createArrayListPool(5, 20, 500);

            // 正确的使用模式
            List<String> list = stringListPool.borrow();
            try {
                // 使用列表进行业务逻辑
                list.add("item1");
                list.add("item2");
                
                // 处理数据...
                processData(list);
                
            } finally {
                // 确保归还对象到池中
                stringListPool.returnObject(list);
            }

            // 查看池的性能统计
            GenericObjectPool.PoolStatistics stats = stringListPool.getStatistics();
            System.out.println("池统计信息: " + stats);
        }

        private static void processData(List<String> data) {
            // 业务逻辑处理
            data.forEach(System.out::println);
        }
    }
} 