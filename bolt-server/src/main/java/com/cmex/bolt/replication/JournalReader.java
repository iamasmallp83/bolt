package com.cmex.bolt.replication;

import com.cmex.bolt.core.BoltConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Journal文件读取器，用于读取journal文件并打包数据
 */
@Slf4j
public class JournalReader {

    private final BoltConfig config;

    public JournalReader(BoltConfig config) {
        this.config = config;
    }

    /**
     * 读取journal文件从第一条记录到指定的replication ID
     *
     * @param firstReplicationID 第一个replication ID
     * @return 打包的journal数据
     */
    public byte[] readJournalToReplicationId(long firstReplicationID) {
        try {
            // 查找合适的journal文件
            Path journalPath = findJournalPath();
            if (journalPath == null) {
                log.warn("No journal files found");
                return new byte[0];
            }

            log.info("Reading journal from {} to replication ID {}", journalPath, firstReplicationID);

            if (config.isBinary()) {
                return readBinaryJournalToReplicationId(journalPath, firstReplicationID);
            } else {
                return readJsonJournalToReplicationId(journalPath, firstReplicationID);
            }

        } catch (Exception e) {
            log.error("Failed to read journal to replication ID {}", firstReplicationID, e);
            return new byte[0];
        }
    }

    /**
     * 查找合适的journal文件
     */
    private Path findJournalPath() throws IOException {
        // 首先尝试默认的journal文件
        Path defaultPath = Path.of(config.journalFilePath());
        if (Files.exists(defaultPath)) {
            return defaultPath;
        }

        // 查找journal目录中的文件
        Path journalDir = Paths.get(config.journalDir());
        if (!Files.exists(journalDir)) {
            return null;
        }

        try (Stream<Path> paths = Files.list(journalDir)) {
            return paths
                    .filter(path -> path.getFileName().toString().startsWith("journal_"))
                    .max((p1, p2) -> {
                        // 按文件名排序，选择最新的
                        return p1.getFileName().toString().compareTo(p2.getFileName().toString());
                    })
                    .orElse(null);
        }
    }

    /**
     * 读取二进制格式的journal文件到指定的replication ID
     */
    private byte[] readBinaryJournalToReplicationId(Path journalPath, long firstReplicationID) throws IOException {
        List<byte[]> journalRecords = new ArrayList<>();

        try (FileChannel journalChannel = FileChannel.open(journalPath, StandardOpenOption.READ)) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

            while (journalChannel.position() < journalChannel.size()) {
                // 读取总长度
                lengthBuffer.clear();
                int bytesRead = journalChannel.read(lengthBuffer);
                if (bytesRead != 4) {
                    log.warn("Incomplete length header at position {}, stopping read", journalChannel.position());
                    break;
                }

                lengthBuffer.flip();
                int totalLength = lengthBuffer.getInt();

                // 验证总长度合理性
                if (totalLength <= 20 || totalLength > 1024 * 1024 + 20) {
                    log.warn("Invalid total length: {}, stopping read", totalLength);
                    break;
                }

                // 读取时间戳（8字节）
                ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(timestampBuffer);
                if (bytesRead != 8) {
                    log.warn("Incomplete timestamp data at position {}, stopping read", journalChannel.position());
                    break;
                }
                timestampBuffer.flip();
                long timestamp = timestampBuffer.getLong();

                // 读取ID（8字节）
                ByteBuffer idBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(idBuffer);
                if (bytesRead != 8) {
                    log.warn("Incomplete ID data at position {}, stopping read", journalChannel.position());
                    break;
                }
                idBuffer.flip();
                long id = idBuffer.getLong();

                // 读取combinedPartitionAndEventType（4字节）
                ByteBuffer partitionBuffer = ByteBuffer.allocate(4);
                bytesRead = journalChannel.read(partitionBuffer);
                if (bytesRead != 4) {
                    log.warn("Incomplete partition data at position {}, stopping read", journalChannel.position());
                    break;
                }
                partitionBuffer.flip();
                int partitionAndEventType = partitionBuffer.getInt();

                // 读取消息内容
                int messageLength = totalLength - 20; // 减去header长度
                ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
                bytesRead = journalChannel.read(messageBuffer);
                if (bytesRead != messageLength) {
                    log.warn("Incomplete message data at position {}, stopping read", journalChannel.position());
                    break;
                }

                // 检查是否到达目标replication ID
                if (id >= firstReplicationID) {
                    log.debug("Reached target replication ID {}, stopping read", firstReplicationID);
                    break;
                }

                // 打包这条记录
                byte[] record = packBinaryRecord(timestamp, id, partitionAndEventType, messageBuffer.array());
                journalRecords.add(record);

                log.debug("Read journal record: id={}, timestamp={}, messageLength={}", id, timestamp, messageLength);
            }
        }

        // 将所有记录合并成一个字节数组
        return combineRecords(journalRecords);
    }

    /**
     * 读取JSON格式的journal文件到指定的replication ID
     */
    private byte[] readJsonJournalToReplicationId(Path journalPath, long firstReplicationID) throws IOException {
        List<byte[]> journalRecords = new ArrayList<>();

        try (var reader = Files.newBufferedReader(journalPath, StandardCharsets.UTF_8)) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue; // 跳过空行
                }

                try {
                    // 简单的JSON解析，不使用Jackson
                    String trimmedLine = line.trim();
                    if (trimmedLine.startsWith("{") && trimmedLine.contains("\"id\":")) {
                        // 简单的ID提取
                        int idStart = trimmedLine.indexOf("\"id\":") + 5;
                        int idEnd = trimmedLine.indexOf(",", idStart);
                        if (idEnd == -1) idEnd = trimmedLine.indexOf("}", idStart);
                        if (idStart > 4 && idEnd > idStart) {
                            String idStr = trimmedLine.substring(idStart, idEnd).trim();
                            long id = Long.parseLong(idStr);

                            // 检查是否到达目标replication ID
                            if (id >= firstReplicationID) {
                                log.debug("Reached target replication ID {}, stopping read", firstReplicationID);
                                break;
                            }

                            // 打包这条记录
                            line += "\n";
                            byte[] record = line.getBytes(StandardCharsets.UTF_8);
                            journalRecords.add(record);

                            log.debug("Read journal record: id={}", id);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Could not parse JSON line, skipping: {}", line.trim());
                    continue;
                }
            }
        }

        // 将所有记录合并成一个字节数组
        return combineRecords(journalRecords);
    }

    /**
     * 打包二进制记录
     */
    private byte[] packBinaryRecord(long timestamp, long id, int partitionAndEventType, byte[] messageData) {
        int totalLength = 20 + messageData.length; // 20 = 8(timestamp) + 8(id) + 4(partitionAndEventType)

        ByteBuffer buffer = ByteBuffer.allocate(4 + totalLength);
        buffer.putInt(totalLength);
        buffer.putLong(timestamp);
        buffer.putLong(id);
        buffer.putInt(partitionAndEventType);
        buffer.put(messageData);

        return buffer.array();
    }

    /**
     * 合并多个记录为一个字节数组
     */
    private byte[] combineRecords(List<byte[]> records) {
        if (records.isEmpty()) {
            return new byte[0];
        }

        // 计算总长度
        int totalLength = 0;
        for (byte[] record : records) {
            totalLength += record.length;
        }

        // 合并所有记录
        ByteBuffer combinedBuffer = ByteBuffer.allocate(totalLength);
        for (byte[] record : records) {
            combinedBuffer.put(record);
        }

        return combinedBuffer.array();
    }
}
