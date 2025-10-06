package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 简单的Binary Journal功能测试
 */
public class SimpleBinaryJournalTest {

    @TempDir
    Path tempDir;

    @Test
    void testBinaryJournalBasicFunctionality() throws Exception {
        // 创建二进制journal配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-binary", true, 300  // enableJournal=true, isBinary=true, snapshotInterval=300
        );

        // 创建JournalHandler
        JournalHandler journalHandler = new JournalHandler(config);

        // 创建测试数据
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
        wrapper.setId(12345L);
        wrapper.setPartition(5);
        wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        wrapper.getBuffer().clear();
        wrapper.getBuffer().writeBytes("Hello Binary Journal!".getBytes());
        wrapper.getBuffer().readerIndex(0);

        // 写入journal
        journalHandler.onEvent(wrapper, 1, true); // endOfBatch=true 触发flush

        // 验证文件存在且不为空
        Path journalPath = Path.of(config.journalFilePath());
        assertTrue(Files.exists(journalPath), "Binary journal file should exist");
        assertTrue(Files.size(journalPath) > 0, "Binary journal file should not be empty");

        System.out.println("Binary journal file created successfully:");
        System.out.println("  Path: " + journalPath);
        System.out.println("  Size: " + Files.size(journalPath) + " bytes");
        System.out.println("  Config: enableJournal=" + config.enableJournal() + ", isBinary=" + config.isBinary());
    }

    @Test
    void testJsonJournalBasicFunctionality() throws Exception {
        // 创建JSON journal配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-json", false, 300  // enableJournal=true, isBinary=false, snapshotInterval=300
        );

        // 创建JournalHandler
        JournalHandler journalHandler = new JournalHandler(config);

        // 创建测试数据
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
        wrapper.setId(67890L);
        wrapper.setPartition(3);
        wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        wrapper.getBuffer().clear();
        wrapper.getBuffer().writeBytes("Hello JSON Journal!".getBytes());
        wrapper.getBuffer().readerIndex(0);

        // 写入journal
        journalHandler.onEvent(wrapper, 1, true); // endOfBatch=true 触发flush

        // 验证文件存在且不为空
        Path journalPath = Path.of(config.journalFilePath());
        assertTrue(Files.exists(journalPath), "JSON journal file should exist");
        assertTrue(Files.size(journalPath) > 0, "JSON journal file should not be empty");

        System.out.println("JSON journal file created successfully:");
        System.out.println("  Path: " + journalPath);
        System.out.println("  Size: " + Files.size(journalPath) + " bytes");
        System.out.println("  Config: enableJournal=" + config.enableJournal() + ", isBinary=" + config.isBinary());
    }

    @Test
    void testJournalDisabled() throws Exception {
        // 创建禁用journal的配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                false, "test-disabled", true, 300  // enableJournal=false, snapshotInterval=300
        );

        // 创建JournalHandler
        JournalHandler journalHandler = new JournalHandler(config);

        // 创建测试数据
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
        wrapper.setId(11111L);
        wrapper.setPartition(1);
        wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        wrapper.getBuffer().clear();
        wrapper.getBuffer().writeBytes("This should not be written".getBytes());
        wrapper.getBuffer().readerIndex(0);

        // 写入journal（应该被忽略）
        journalHandler.onEvent(wrapper, 1, true);

        // 验证文件不存在
        Path journalPath = Path.of(config.journalFilePath());
        assertFalse(Files.exists(journalPath), "Journal file should not exist when disabled");

        System.out.println("Journal correctly disabled - no file created");
    }
}
