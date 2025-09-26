package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 复制功能测试
 */
public class ReplicationTest {
    
    @Test
    public void testReplicationState() {
        ReplicationState state = new ReplicationState();
        
        // 注册从节点
        state.registerSlave("slave1", "localhost", 9091);
        state.registerSlave("slave2", "localhost", 9092);
        
        // 检查从节点数量
        assertEquals(2, state.getAllSlaves().size());
        
        // 创建批次跟踪器
        ReplicationState.BatchAckTracker tracker = state.createBatchTracker(5000, 100, 200);
        
        // 检查批次跟踪器
        assertNotNull(tracker);
        assertEquals(2, tracker.getPendingCount());
        
        // 模拟确认
        assertTrue(state.acknowledgeBatch(tracker.getBatchId(), "slave1"));
        assertEquals(1, tracker.getPendingCount());
        
        assertTrue(state.acknowledgeBatch(tracker.getBatchId(), "slave2"));
        assertEquals(0, tracker.getPendingCount());
        assertTrue(tracker.isAllAcknowledged());
    }
    
    @Test
    public void testBoltConfig() {
        BoltConfig config = new BoltConfig(
                9090, false, 4, 1024, 512, 512, 
                true, 9091, "journal", false,
                false, "localhost", 9090, 9092, true, 100, 5000, true
        );
        
        assertFalse(config.isSlave());
        assertEquals("localhost", config.masterHost());
        assertEquals(9090, config.masterPort());
        assertEquals(9092, config.replicationPort());
        assertTrue(config.enableReplication());
        assertEquals(100, config.batchSize());
        assertEquals(5000, config.batchTimeoutMs());
    }
    
    @Test
    public void testSlaveConfig() {
        BoltConfig slaveConfig = new BoltConfig(
                9091, false, 4, 1024, 512, 512, 
                true, 9092, "slave-journal", false,
                true, "localhost", 9090, 9093, true, 50, 3000, true
        );
        
        assertTrue(slaveConfig.isSlave());
        assertEquals("localhost", slaveConfig.masterHost());
        assertEquals(9090, slaveConfig.masterPort());
        assertEquals(50, slaveConfig.batchSize());
        assertEquals(3000, slaveConfig.batchTimeoutMs());
    }
}
