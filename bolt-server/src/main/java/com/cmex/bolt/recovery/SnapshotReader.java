package com.cmex.bolt.recovery;

import com.cmex.bolt.core.BoltConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class SnapshotReader {

    private final BoltConfig config;
    private final ObjectMapper objectMapper;

    public SnapshotReader(BoltConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 查找最新的snapshot文件
     */
    public SnapshotInfo findLatestSnapshot() throws IOException {
        Path sequencerSnapshotDir = Paths.get(config.boltHome(), "sequencer_snapshots");
        Path matchingSnapshotDir = Paths.get(config.boltHome(), "matching_snapshots");
        
        // 确保目录存在
        Files.createDirectories(sequencerSnapshotDir);
        Files.createDirectories(matchingSnapshotDir);

        long latestTimestamp = findLatestSnapshotTimestamp(sequencerSnapshotDir);
        
        if (latestTimestamp == -1) {
            log.info("No snapshot files found");
            return null;
        }

        // 构建新目录结构下的文件路径
        Path timestampDir = sequencerSnapshotDir.resolve(String.valueOf(latestTimestamp));
        Path matchingTimestampDir = matchingSnapshotDir.resolve(String.valueOf(latestTimestamp));
        
        return new SnapshotInfo(
            latestTimestamp,
            timestampDir.resolve("account_0"),
            timestampDir.resolve("currency_0"),
            timestampDir.resolve("symbol_0"),
            matchingTimestampDir.resolve("matching_0")
        );
    }

    /**
     * 查找最新的matching snapshot文件
     */
    public long findLatestMatchingSnapshotTimestamp() throws IOException {
        Path matchingSnapshotDir = Paths.get(config.boltHome(), "matching_snapshots");
        
        if (!Files.exists(matchingSnapshotDir)) {
            return -1;
        }

        try (Stream<Path> paths = Files.list(matchingSnapshotDir)) {
            return paths
                .filter(Files::isDirectory)
                .map(path -> {
                    try {
                        return Long.parseLong(path.getFileName().toString());
                    } catch (NumberFormatException e) {
                        return -1L;
                    }
                })
                .filter(timestamp -> timestamp > 0)
                .max(Long::compareTo)
                .orElse(-1L);
        }
    }

    /**
     * 读取账户数据
     */
    public Map<String, Object> readAccountData(Path accountFile) throws IOException {
        if (!Files.exists(accountFile)) {
            log.warn("Account snapshot file not found: {}", accountFile);
            return null;
        }
        
        JsonNode rootNode = objectMapper.readTree(accountFile.toFile());
        JsonNode accountsNode = rootNode.get("accounts");
        
        if (accountsNode != null) {
            return objectMapper.convertValue(accountsNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        }
        return null;
    }

    /**
     * 读取货币数据
     */
    public Map<String, Object> readCurrencyData(Path currencyFile) throws IOException {
        if (!Files.exists(currencyFile)) {
            log.warn("Currency snapshot file not found: {}", currencyFile);
            return null;
        }
        
        JsonNode rootNode = objectMapper.readTree(currencyFile.toFile());
        JsonNode currenciesNode = rootNode.get("currencies");
        
        if (currenciesNode != null) {
            return objectMapper.convertValue(currenciesNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        }
        return null;
    }

    /**
     * 读取交易对数据
     */
    public Map<String, Object> readSymbolData(Path symbolFile) throws IOException {
        if (!Files.exists(symbolFile)) {
            log.warn("Symbol snapshot file not found: {}", symbolFile);
            return null;
        }
        
        JsonNode rootNode = objectMapper.readTree(symbolFile.toFile());
        JsonNode symbolsNode = rootNode.get("symbols");
        
        if (symbolsNode != null) {
            return objectMapper.convertValue(symbolsNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        }
        return null;
    }

    /**
     * 读取matching数据
     */
    public Map<String, Object> readMatchingData(Path matchingFile) throws IOException {
        if (!Files.exists(matchingFile)) {
            log.warn("Matching snapshot file not found: {}", matchingFile);
            return null;
        }
        
        JsonNode rootNode = objectMapper.readTree(matchingFile.toFile());
        JsonNode symbolsNode = rootNode.get("symbols");
        
        if (symbolsNode != null) {
            return objectMapper.convertValue(symbolsNode, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        }
        return null;
    }

    private long findLatestSnapshotTimestamp(Path sequencerSnapshotDir) throws IOException {
        if (!Files.exists(sequencerSnapshotDir)) {
            return -1;
        }

        try (Stream<Path> paths = Files.list(sequencerSnapshotDir)) {
            return paths
                .filter(Files::isDirectory)
                .map(path -> {
                    try {
                        return Long.parseLong(path.getFileName().toString());
                    } catch (NumberFormatException e) {
                        return -1L;
                    }
                })
                .filter(timestamp -> timestamp > 0)
                .max(Long::compareTo)
                .orElse(-1L);
        }
    }

    private long extractTimestampFromPath(Path path) {
        try {
            String filename = path.getFileName().toString();
            // 对于新目录结构，直接解析目录名作为时间戳
            return Long.parseLong(filename);
        } catch (NumberFormatException e) {
            log.warn("Could not extract timestamp from path: {}", path, e);
            return -1;
        }
    }

    /**
     * Snapshot信息封装类
     */
    public static class SnapshotInfo {
        private final long timestamp;
        private final Path accountFile;
        private final Path currencyFile;
        private final Path symbolFile;
        private final Path matchingFile;

        public SnapshotInfo(long timestamp, Path accountFile, Path currencyFile, Path symbolFile, Path matchingFile) {
            this.timestamp = timestamp;
            this.accountFile = accountFile;
            this.currencyFile = currencyFile;
            this.symbolFile = symbolFile;
            this.matchingFile = matchingFile;
        }

        public long getTimestamp() { return timestamp; }
        public Path getAccountFile() { return accountFile; }
        public Path getCurrencyFile() { return currencyFile; }
        public Path getSymbolFile() { return symbolFile; }
        public Path getMatchingFile() { return matchingFile; }
    }
}
