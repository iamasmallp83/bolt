package com.cmex.bolt.performance;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.EnvoyServer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 增强版订单性能测试
 * 使用SystemCompletionDetector来准确判断复杂异步系统的完成状态
 */
@Tag("performance")
public class TestCorePerformance {

    private static EnvoyServer service;

    @BeforeAll
    static void setUp() {
        service = new EnvoyServer(new BoltConfig(9090, true, 10,
                1024 * 1024 * 8, 1024 * 1024 * 4, 1024 * 1024 * 4, true, 9091));
    }

    @Test
    public void test() throws Exception {
    }
}