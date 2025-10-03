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
 * Binary Journal核心功能测试
 */
public class BinaryJournalCoreTest {

    @TempDir
    Path tempDir;

    @Test
    void testBinaryJournalWrite() throws Exception {
        // 创建二进制journal配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-binary", true  // enableJournal=true, isBinary=true
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

        System.out.println("✅ Binary journal write test passed:");
        System.out.println("  Path: " + journalPath);
        System.out.println("  Size: " + Files.size(journalPath) + " bytes");
        System.out.println("  Config: enableJournal=" + config.enableJournal() + ", isBinary=" + config.isBinary());
    }

    @Test
    void testJsonJournalWrite() throws Exception {
        // 创建JSON journal配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-json", false  // enableJournal=true, isBinary=false
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

        System.out.println("✅ JSON journal write test passed:");
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
                false, "test-disabled", true  // enableJournal=false
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

        System.out.println("✅ Journal disabled test passed - no file created");
    }

    @Test
    void testBinaryVsJsonSizeComparison() throws Exception {
        // 创建二进制journal配置
        BoltConfig binaryConfig = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-binary", true  // enableJournal=true, isBinary=true
        );

        // 创建JSON journal配置
        BoltConfig jsonConfig = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-json", false  // enableJournal=true, isBinary=false
        );

        // 创建JournalHandler
        JournalHandler binaryHandler = new JournalHandler(binaryConfig);
        JournalHandler jsonHandler = new JournalHandler(jsonConfig);

        // 创建相同的测试数据
        String testData = "This is a test message for size comparison";
        
        NexusWrapper binaryWrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
        binaryWrapper.setId(12345L);
        binaryWrapper.setPartition(5);
        binaryWrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        binaryWrapper.getBuffer().clear();
        binaryWrapper.getBuffer().writeBytes(testData.getBytes());
        binaryWrapper.getBuffer().readerIndex(0);

        NexusWrapper jsonWrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
        jsonWrapper.setId(12345L);
        jsonWrapper.setPartition(5);
        jsonWrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        jsonWrapper.getBuffer().clear();
        jsonWrapper.getBuffer().writeBytes(testData.getBytes());
        jsonWrapper.getBuffer().readerIndex(0);

        // 写入journal
        binaryHandler.onEvent(binaryWrapper, 1, true);
        jsonHandler.onEvent(jsonWrapper, 1, true);

        // 比较文件大小
        Path binaryPath = Path.of(binaryConfig.journalFilePath());
        Path jsonPath = Path.of(jsonConfig.journalFilePath());
        
        long binarySize = Files.size(binaryPath);
        long jsonSize = Files.size(jsonPath);

        System.out.println("✅ Size comparison test passed:");
        System.out.println("  Binary journal size: " + binarySize + " bytes");
        System.out.println("  JSON journal size: " + jsonSize + " bytes");
        System.out.println("  Binary is " + String.format("%.2f", (double) jsonSize / binarySize) + "x smaller");

        // 二进制应该更小
        assertTrue(binarySize < jsonSize, "Binary journal should be smaller than JSON");
    }

    @Test
    void testMultipleEventsWrite() throws Exception {
        // 创建二进制journal配置
        BoltConfig config = new BoltConfig(
                1, tempDir.toString(), 9090, false, 4, 1024, 512, 512,
                true, 9091, true, "localhost", 9090, 9092, 100, 5000,
                true, "test-multiple", true  // enableJournal=true, isBinary=true
        );

        // 创建JournalHandler
        JournalHandler journalHandler = new JournalHandler(config);

        // 写入多个事件
        for (int i = 1; i <= 5; i++) {
            NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, 1024);
            wrapper.setId(i);
            wrapper.setPartition(i % 3);
            wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
            wrapper.getBuffer().clear();
            wrapper.getBuffer().writeBytes(("Event " + i).getBytes());
            wrapper.getBuffer().readerIndex(0);

            journalHandler.onEvent(wrapper, i, i == 5); // 最后一个事件触发flush
        }

        // 验证文件存在且不为空
        Path journalPath = Path.of(config.journalFilePath());
        assertTrue(Files.exists(journalPath), "Journal file should exist");
        assertTrue(Files.size(journalPath) > 0, "Journal file should not be empty");

        System.out.println("✅ Multiple events write test passed:");
        System.out.println("  Path: " + journalPath);
        System.out.println("  Size: " + Files.size(journalPath) + " bytes");
        System.out.println("  Events written: 5");
    }
}
