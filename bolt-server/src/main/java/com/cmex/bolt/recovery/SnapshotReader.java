package com.cmex.bolt.recovery;

import com.cmex.bolt.core.BoltConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

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
        Path snapshotDir = Paths.get(config.boltHome(), "snapshots");
        
        // 确保目录存在
        Files.createDirectories(snapshotDir);

        long latestTimestamp = findLatestSnapshotTimestamp(snapshotDir);
        
        if (latestTimestamp == -1) {
            log.info("No snapshot files found");
            return null;
        }

        // 构建新目录结构下的文件路径
        Path timestampDir = snapshotDir.resolve(String.valueOf(latestTimestamp));
        
        return new SnapshotInfo(
            latestTimestamp,
            timestampDir
        );
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
     * 将最新的快照文件夹打包压缩为字节数组
     * 
     * @return 压缩后的字节数组，如果快照不存在或打包失败则返回null
     */
    public byte[] packSnapshot() {
        try {
            SnapshotInfo snapshotInfo = findLatestSnapshot();
            if (snapshotInfo == null || snapshotInfo.getTimestampDir() == null) {
                log.info("No snapshot available for packing");
                return null;
            }
            
            Path snapshotDir = snapshotInfo.getTimestampDir();
            if (!Files.exists(snapshotDir) || !Files.isDirectory(snapshotDir)) {
                log.warn("Snapshot directory does not exist: {}", snapshotDir);
                return null;
            }
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ZipOutputStream zos = new ZipOutputStream(baos)) {
                packDirectory(snapshotDir, snapshotDir.getFileName().toString(), zos);
            }
            
            byte[] result = baos.toByteArray();
            log.info("Successfully packed snapshot from {} to {} bytes", snapshotDir, result.length);
            return result;
            
        } catch (IOException e) {
            log.error("Failed to pack latest snapshot", e);
            return null;
        }
    }
    
    /**
     * 解压缩快照数据到本地快照目录
     * 
     * @param snapshotData 压缩的快照数据
     * @return 解压缩后的快照目录路径，如果解压缩失败则返回null
     */
    public Path extractSnapshot(byte[] snapshotData) {
        if (snapshotData == null || snapshotData.length == 0) {
            log.warn("Snapshot data is null or empty");
            return null;
        }
        
        try {
            // 创建快照目录
            Path snapshotDir = Paths.get(config.boltHome(), "snapshots");
            Files.createDirectories(snapshotDir);
            
            // 创建临时目录用于解压缩
            Path tempDir = Files.createTempDirectory("snapshot_extract_");
            
            try (ByteArrayInputStream bais = new ByteArrayInputStream(snapshotData);
                 ZipInputStream zis = new ZipInputStream(bais)) {
                
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    Path entryPath = tempDir.resolve(entry.getName());
                    
                    // 确保父目录存在
                    Files.createDirectories(entryPath.getParent());
                    
                    // 解压缩文件
                    try (FileOutputStream fos = new FileOutputStream(entryPath.toFile())) {
                        byte[] buffer = new byte[8192];
                        int length;
                        while ((length = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, length);
                        }
                    }
                    
                    log.debug("Extracted file: {}", entry.getName());
                }
            }
            
            // 将解压缩的文件移动到快照目录
            // 假设压缩包中包含时间戳目录
            Path[] extractedDirs = Files.list(tempDir)
                    .filter(Files::isDirectory)
                    .toArray(Path[]::new);
            
            if (extractedDirs.length == 0) {
                log.warn("No directories found in extracted snapshot");
                return null;
            }
            
            // 使用第一个找到的目录（通常是时间戳目录）
            Path sourceDir = extractedDirs[0];
            Path targetDir = snapshotDir.resolve(sourceDir.getFileName());
            
            // 如果目标目录已存在，先删除
            if (Files.exists(targetDir)) {
                deleteDirectory(targetDir);
            }
            
            // 移动目录
            Files.move(sourceDir, targetDir);
            
            // 清理临时目录
            deleteDirectory(tempDir);
            
            log.info("Successfully extracted snapshot to: {}", targetDir);
            return targetDir;
            
        } catch (IOException e) {
            log.error("Failed to extract snapshot", e);
            return null;
        }
    }
    
    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path dir) throws IOException {
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a)) // 反向排序，先删除文件再删除目录
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            log.warn("Failed to delete: {}", path, e);
                        }
                    });
        }
    }
    
    /**
     * 检查快照文件夹是否存在且不为空
     * 
     * @param snapshotPath 快照文件夹路径
     * @return 如果快照存在且包含文件则返回true
     */
    public boolean hasSnapshot(String snapshotPath) {
        if (snapshotPath == null || snapshotPath.trim().isEmpty()) {
            return false;
        }
        
        Path snapshotDir = Paths.get(snapshotPath);
        if (!Files.exists(snapshotDir) || !Files.isDirectory(snapshotDir)) {
            return false;
        }
        
        try {
            return Files.walk(snapshotDir)
                    .anyMatch(path -> Files.isRegularFile(path));
        } catch (IOException e) {
            log.error("Failed to check snapshot directory: {}", snapshotPath, e);
            return false;
        }
    }
    
    /**
     * 递归打包目录到ZipOutputStream
     */
    private void packDirectory(Path dir, String baseName, ZipOutputStream zos) throws IOException {
        Files.walk(dir)
                .filter(path -> !Files.isDirectory(path)) // 只处理文件，不处理目录
                .forEach(file -> {
                    try {
                        String relativePath = dir.getParent() != null 
                            ? dir.getParent().relativize(file).toString()
                            : file.getFileName().toString();
                        
                        ZipEntry entry = new ZipEntry(relativePath);
                        zos.putNextEntry(entry);
                        
                        try (FileInputStream fis = new FileInputStream(file.toFile())) {
                            byte[] buffer = new byte[8192];
                            int length;
                            while ((length = fis.read(buffer)) > 0) {
                                zos.write(buffer, 0, length);
                            }
                        }
                        
                        zos.closeEntry();
                        log.debug("Added file to snapshot: {}", relativePath);
                        
                    } catch (IOException e) {
                        log.error("Failed to add file to snapshot: {}", file, e);
                        throw new RuntimeException(e);
                    }
                });
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

    /**
     * Snapshot信息封装类
     */
    public static class SnapshotInfo {
        private final long timestamp;
        private final Path timestampDir;

        public SnapshotInfo(long timestamp, Path timestampDir) {
            this.timestamp = timestamp;
            this.timestampDir = timestampDir;
        }

        public long getTimestamp() { return timestamp; }
        
        public Path getTimestampDir() { return timestampDir; }
        
        /**
         * 获取指定partition的账户文件路径
         */
        public Path getAccountFile(int partition) {
            return timestampDir.resolve(String.format("account_%d", partition));
        }
        
        /**
         * 获取指定partition的货币文件路径
         */
        public Path getCurrencyFile(int partition) {
            return timestampDir.resolve(String.format("currency_%d", partition));
        }
        
        /**
         * 获取指定partition的交易对文件路径
         */
        public Path getSymbolFile(int partition) {
            return timestampDir.resolve(String.format("symbol_%d", partition));
        }
        
        /**
         * 获取指定partition的matching文件路径
         */
        public Path getMatchingFile(int partition) {
            return timestampDir.resolve(String.format("matching_%d", partition));
        }
    }
}
