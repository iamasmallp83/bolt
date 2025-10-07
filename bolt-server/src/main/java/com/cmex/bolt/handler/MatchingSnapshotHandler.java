package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.repository.impl.SymbolRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class MatchingSnapshotHandler {

    private final int partition;
    private final BoltConfig config;
    private final SymbolRepository symbolRepository;
    private final ObjectMapper objectMapper;

    public MatchingSnapshotHandler(int partition, BoltConfig config, SymbolRepository symbolRepository) {
        this.partition = partition;
        this.config = config;
        this.symbolRepository = symbolRepository;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 处理matching snapshot事件，持久化OrderBook数据
     */
    public void handleSnapshot(Nexus.NexusEvent.Reader reader) {
        Nexus.Snapshot.Reader snapshot = reader.getPayload().getSnapshot();
        long timestamp = snapshot.getTimestamp();
        log.info("Processing matching snapshot event with timestamp: {}", timestamp);

        try {
            // 持久化OrderBook数据
            persistOrderBookData(timestamp);

            log.info("Matching snapshot completed successfully with timestamp: {}", timestamp);
        } catch (Exception e) {
            log.error("Failed to process matching snapshot with timestamp: {}", timestamp, e);
        }
    }

    /**
     * 持久化OrderBook数据为snapshots/{timestamp}/symbols_{partition}文件
     */
    private void persistOrderBookData(long timestamp) throws IOException {
        // 创建matching snapshot目录结构：snapshots/{timestamp}/
        Path baseMatchingSnapshotDir = Paths.get(config.boltHome(), "snapshots");
        Path timestampSnapshotDir = baseMatchingSnapshotDir.resolve(String.valueOf(timestamp));
        Files.createDirectories(timestampSnapshotDir);

        // 生成文件名：symbols_0
        String filename = String.format("symbol_%d", this.partition);
        Path filePath = timestampSnapshotDir.resolve(filename);

        // 获取所有Symbol的OrderBook数据
        Map<String, Object> matchingData = new java.util.HashMap<>();
        matchingData.put("timestamp", timestamp);
        matchingData.put("symbols", symbolRepository.getAllData());

        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            objectMapper.writeValue(writer, matchingData);
        }

        log.debug("Matching data persisted to: {}", filePath);
    }
}
