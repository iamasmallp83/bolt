package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.recovery.SnapshotReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.RingBuffer;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.io.IOException;

/**
 * JournalReplayer 负责从 journal 文件重放事件到 RingBuffer
 * 用于系统启动时恢复状态或故障恢复
 */
@Slf4j
public class JournalReplayer {

    private final BoltConfig config;
    private final RingBuffer<NexusWrapper> targetRingBuffer;

    // 性能监控和配置
    private final AtomicLong totalReplayedEvents = new AtomicLong(0);
    private final AtomicLong totalReplayTime = new AtomicLong(0);

    // 配置相关的常量
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024; // 1MB
    private static final int DEFAULT_LOG_INTERVAL = 10000;

    // JSON处理
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JournalReplayer(RingBuffer<NexusWrapper> targetRingBuffer, BoltConfig config) {
        this.config = config;
        this.targetRingBuffer = targetRingBuffer;

        log.info("JournalReplayer initialized with config: port={}, isProd={}, journalPath={}, isBinary={}",
                config.port(), config.isProd(), config.journalFilePath(), config.isBinary());
    }

    public long replayFromJournal() {
        return replayFromJournal(null);
    }

    /**
     * 从Journal回放事件，支持从指定的snapshot时间戳之后开始回放
     * @param snapshotTimestamp 快照时间戳，如果为null则从头开始回放
     * @return 重放的事件数量
     */
    public long replayFromJournal(Long snapshotTimestamp) {
        // 如果禁用日志，跳过 journal 重放
        if (!config.enableJournal()) {
            log.info("Journal disabled, skipping journal replay");
            return 0;
        }
        
        long startTime = System.currentTimeMillis();

        try {
            // 查找合适的journal文件进行回放
            Path journalPath = findJournalPathToReplay(snapshotTimestamp);
            
            if (journalPath == null) {
                log.info("No journal files found for replay");
                return 0;
            }

            long fileSize = Files.size(journalPath);
            log.info("Starting journal replay: file={}, size={} bytes, format={}, fromSnapshot={}",
                    journalPath, fileSize, config.isBinary() ? "binary" : "JSON", snapshotTimestamp);

            long replayedEvents;
            if (config.isBinary()) {
                replayedEvents = replayFromBinaryJournal(journalPath, snapshotTimestamp);
            } else {
                replayedEvents = replayFromJsonJournal(journalPath, snapshotTimestamp);
            }

            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            totalReplayTime.addAndGet(duration);
            totalReplayedEvents.addAndGet(replayedEvents);

            log.info("Journal replay completed: {} events in {} ms ({} events/sec)",
                    replayedEvents, duration,
                    duration > 0 ? (replayedEvents * 1000 / duration) : 0);

            return replayedEvents;

        } catch (Exception e) {
            log.error("Failed to replay journal: {}", e.getMessage(), e);
            if (config.isProd()) {
                throw new RuntimeException("Journal replay failed in production", e);
            } else {
                log.warn("Continuing in non-production mode despite replay failure");
                return 0;
            }
        }
    }

    /**
     * 查找合适的journal文件进行回放
     * @param snapshotTimestamp 快照时间戳，如果为null则使用默认journal文件
     * @return 合适的journal文件路径，如果没有找到则返回null
     */
    private Path findJournalPathToReplay(Long snapshotTimestamp) throws IOException {
        if (snapshotTimestamp == null) {
            // 如果没有快照时间戳，使用默认的journal文件
            Path defaultPath = Path.of(config.journalFilePath());
            if (Files.exists(defaultPath)) {
                return defaultPath;
            }
            return null;
        }

        // 查找包含快照时间戳的journal文件
        Path journalDir = Paths.get(config.journalDir());
        if (!Files.exists(journalDir)) {
            return null;
        }

        try (Stream<Path> paths = Files.list(journalDir)) {
            return paths
                .filter(path -> path.getFileName().toString().startsWith("journal_"))
                .filter(path -> {
                    // 检查文件名是否包含大于快照时间戳的时间
                    long fileTimestamp = extractTimestampFromJournalPath(path);
                    return fileTimestamp > snapshotTimestamp;
                })
                .min((p1, p2) -> {
                    long t1 = extractTimestampFromJournalPath(p1);
                    long t2 = extractTimestampFromJournalPath(p2);
                    return Long.compare(t1, t2);
                })
                .orElse(null);
        }
    }

    /**
     * 从journal文件路径中提取时间戳
     */
    private long extractTimestampFromJournalPath(Path path) {
        try {
            String filename = path.getFileName().toString();
            // 格式：journal_1699123456789.data
            int startIndex = filename.indexOf('_') + 1;
            int endIndex = filename.lastIndexOf('.');
            
            if (startIndex > 0 && endIndex > startIndex) {
                String timestampStr = filename.substring(startIndex, endIndex);
                return Long.parseLong(timestampStr);
            }
        } catch (Exception e) {
            log.warn("Could not extract timestamp from journal path: {}", path, e);
        }
        return -1;
    }

    /**
     * 从二进制格式的 journal 文件重放事件
     *
     * @param journalFilePath journal 文件路径
     * @return 重放的事件数量
     */
    private long replayFromBinaryJournal(Path journalFilePath) {
        return replayFromBinaryJournal(journalFilePath, null);
    }

    /**
     * 从二进制格式的 journal 文件重放事件，支持从指定时间点开始
     *
     * @param journalFilePath journal 文件路径
     * @param snapshotTimestamp 快照时间戳，如果为null则从头开始
     * @return 重放的事件数量
     */
    private long replayFromBinaryJournal(Path journalFilePath, Long snapshotTimestamp) {
        try (FileChannel journalChannel = FileChannel.open(journalFilePath, StandardOpenOption.READ)) {
            long totalEvents = 0;
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            long batchStartTime = System.currentTimeMillis();

            log.info("Starting binary journal replay from: {}", journalFilePath);

            while (journalChannel.position() < journalChannel.size()) {
                // 读取总长度（包含ID、partition和消息内容）
                lengthBuffer.clear();
                int bytesRead = journalChannel.read(lengthBuffer);
                if (bytesRead != 4) {
                    log.warn("Incomplete length header at position {}, stopping replay", journalChannel.position());
                    break;
                }

                lengthBuffer.flip();
                int totalLength = lengthBuffer.getInt();

                // 验证总长度合理性 - 使用配置的最大消息大小
                int maxMessageSize = getMaxMessageSize();
                if (totalLength <= 20 || totalLength > maxMessageSize + 20) { // 20 = 8(timestamp) + 8(ID) + 4(combinedPartitionAndEventType)
                    log.warn("Invalid total length: {} (max allowed: {}), stopping replay",
                            totalLength, maxMessageSize + 20);
                    break;
                }

                // 读取时间戳（8字节）
                ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(timestampBuffer);
                if (bytesRead != 8) {
                    log.warn("Incomplete timestamp data at position {}, stopping replay", journalChannel.position());
                    break;
                }
                timestampBuffer.flip();
                long timestamp = timestampBuffer.getLong();

                // 如果指定了snapshot时间戳，跳过snapshot之前的事件
                if (snapshotTimestamp != null && timestamp <= snapshotTimestamp) {
                    // 跳过剩余的消息内容，移动到下一个事件
                    journalChannel.position(journalChannel.position() + (totalLength - 8)); // 8 = 已读取的时间戳
                    continue;
                }

                // 读取ID（8字节）
                ByteBuffer idBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(idBuffer);
                if (bytesRead != 8) {
                    log.warn("Incomplete ID data at position {}, stopping replay", journalChannel.position());
                    break;
                }
                idBuffer.flip();
                long id = idBuffer.getLong();

                // 读取合并的partition和eventType（4字节）
                ByteBuffer combinedBuffer = ByteBuffer.allocate(4);
                bytesRead = journalChannel.read(combinedBuffer);
                if (bytesRead != 4) {
                    log.warn("Incomplete combinedPartitionAndEventType data at position {}, stopping replay", journalChannel.position());
                    break;
                }
                combinedBuffer.flip();
                int combinedPartitionAndEventType = combinedBuffer.getInt();

                // 计算消息内容长度
                int messageLength = totalLength - 20; // 总长度 - 时间戳(8) - ID(8) - combinedPartitionAndEventType(4)

                // 读取消息内容
                ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
                bytesRead = journalChannel.read(messageBuffer);
                if (bytesRead != messageLength) {
                    log.warn("Incomplete message data at position {}, stopping replay", journalChannel.position());
                    break;
                }

                // 发布事件到 RingBuffer，包含ID、partition和eventType信息
                messageBuffer.flip();
                publishEventToRingBuffer(messageBuffer, combinedPartitionAndEventType);
                totalEvents++;

                // 批量处理日志记录
                if (totalEvents % getLogInterval() == 0) {
                    long currentTime = System.currentTimeMillis();
                    long batchDuration = currentTime - batchStartTime;
                    log.info("Replayed {} binary events (batch: {} events in {} ms)",
                            totalEvents, getLogInterval(), batchDuration);
                    batchStartTime = currentTime;
                    TimeUnit.MILLISECONDS.sleep(100);
                }

            }

            log.info("Binary journal replay completed. Total events replayed: {}", totalEvents);
            return totalEvents;

        } catch (Exception e) {
            log.error("Failed to replay from binary journal: {}", journalFilePath, e);
            return 0;
        }
    }

    /**
     * 从JSON格式的 journal 文件重放事件
     *
     * @param journalFilePath journal 文件路径
     * @return 重放的事件数量
     */
    private long replayFromJsonJournal(Path journalFilePath) {
        return replayFromJsonJournal(journalFilePath, null);
    }

    /**
     * 从JSON格式的 journal 文件重放事件，支持从指定时间点开始
     *
     * @param journalFilePath journal 文件路径
     * @param snapshotTimestamp 快照时间戳，如果为null则从头开始
     * @return 重放的事件数量
     */
    private long replayFromJsonJournal(Path journalFilePath, Long snapshotTimestamp) {
        long totalEvents = 0;
        long batchStartTime = System.currentTimeMillis();
        int logInterval = getLogInterval();

        log.info("Starting JSON journal replay from: {} (log interval: {})", journalFilePath, logInterval);

        try (BufferedReader reader = Files.newBufferedReader(journalFilePath, StandardCharsets.UTF_8)) {
            String line;
            int skippedLines = 0;

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue; // 跳过空行
                }

                // 如果指定了snapshot时间戳，先检查这行数据的时间戳
                if (snapshotTimestamp != null) {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(line.trim());
                        long eventTimestamp = jsonNode.has("timestamp") ? jsonNode.get("timestamp").asLong() : 0;
                        
                        if (eventTimestamp <= snapshotTimestamp) {
                            // 跳过snapshot之前的事件
                            skippedLines++;
                            continue;
                        }
                    } catch (Exception e) {
                        log.warn("Could not parse timestamp from JSON line, skipping: {}", line.trim());
                        skippedLines++;
                        continue;
                    }
                }

                // 解析JSON并转换为Cap'n Proto数据
                ByteBuffer messageBuffer = parseJsonToCapnProto(line);
                if (messageBuffer != null) {
                    // parseJsonToCapnProto 已经发布了事件到 RingBuffer，这里只需要统计
                    totalEvents++;

                    // 批量处理日志记录
                    if (totalEvents % logInterval == 0) {
                        long currentTime = System.currentTimeMillis();
                        long batchDuration = currentTime - batchStartTime;
                        log.info("Replayed {} JSON events (batch: {} events in {} ms, skipped: {})",
                                totalEvents, logInterval, batchDuration, skippedLines);
                        batchStartTime = currentTime;
                        TimeUnit.MILLISECONDS.sleep(100);
                    }
                }
            }

            log.info("JSON journal replay completed. Total events replayed: {}, skipped: {}",
                    totalEvents, skippedLines);
            return totalEvents;

        } catch (Exception e) {
            log.error("Failed to replay from JSON journal: {}", journalFilePath, e);
            return 0;
        }
    }

    /**
     * 解析JSON字符串并转换为Cap'n Proto格式的ByteBuffer
     *
     * @param jsonLine JSON格式的事件数据
     * @return Cap'n Proto格式的ByteBuffer，解析失败时返回null
     */
    private ByteBuffer parseJsonToCapnProto(String jsonLine) {
        try {
            // 使用Jackson解析JSON
            String trimmedLine = jsonLine.trim();
            if (!trimmedLine.startsWith("{") || (!trimmedLine.endsWith("}") && !trimmedLine.endsWith("}\n"))) {
                log.warn("Invalid JSON format: {}", jsonLine);
                return null;
            }

            // 移除末尾的换行符（如果存在）
            if (trimmedLine.endsWith("\n")) {
                trimmedLine = trimmedLine.substring(0, trimmedLine.length() - 1);
            }

            // 使用Jackson解析JSON
            JsonNode rootNode = objectMapper.readTree(trimmedLine);

            // 提取data字段
            JsonNode dataNode = rootNode.get("data");
            if (dataNode == null) {
                log.warn("Missing data field in JSON: {}", trimmedLine);
                return null;
            }

            // 将JSON数据转换为Cap'n Proto格式
            ByteBuffer capnProtoBuffer = convertJsonDataToCapnProto(dataNode);

            if (capnProtoBuffer != null) {
                // 直接发布事件到RingBuffer，不返回ByteBuffer避免重复处理
                targetRingBuffer.publishEvent((wrapper, sequence) -> {
                    ByteBuf buffer = wrapper.getBuffer();
                    buffer.clear();
                    buffer.writeBytes(capnProtoBuffer);

                    wrapper.setId(rootNode.get("id").asLong());
                    wrapper.setPartition(rootNode.get("partition").asInt());
                    wrapper.setEventType(NexusWrapper.EventType.JOURNAL);
                });

                return capnProtoBuffer; // 返回用于统计，但不会重复处理
            }

            return null;

        } catch (Exception e) {
            log.warn("Failed to parse JSON to Cap'n Proto: {}", jsonLine, e);
            return null;
        }
    }


    /**
     * 将JSON数据转换为Cap'n Proto格式
     * 使用Jackson解析JSON，然后使用Transfer类进行序列化
     */
    private ByteBuffer convertJsonDataToCapnProto(JsonNode dataNode) {
        try {
            // 提取payloadType
            JsonNode payloadTypeNode = dataNode.get("payloadType");
            if (payloadTypeNode == null) {
                log.warn("Missing payloadType in data JSON: {}", dataNode);
                return null;
            }

            String payloadType = payloadTypeNode.asText();
            JsonNode payloadNode = dataNode.get("payload");
            if (payloadNode == null) {
                log.warn("Missing payload in data JSON: {}", dataNode);
                return null;
            }

            log.debug("Converting JSON payload type: {} to Cap'n Proto", payloadType);

            // 使用Transfer类来创建Cap'n Proto消息
            com.cmex.bolt.domain.Transfer transfer = new com.cmex.bolt.domain.Transfer();
            io.grpc.netty.shaded.io.netty.buffer.ByteBuf buffer = io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(1024);

            try {
                // 根据payloadType创建相应的Cap'n Proto消息
                switch (payloadType) {
                    case "INCREASE" -> {
                        int accountId = payloadNode.get("accountId").asInt();
                        int currencyId = payloadNode.get("currencyId").asInt();
                        String amount = payloadNode.get("amount").asText();

                        // 创建IncreaseRequest并转换为Cap'n Proto
                        com.cmex.bolt.Envoy.IncreaseRequest request = com.cmex.bolt.Envoy.IncreaseRequest.newBuilder()
                                .setAccountId(accountId)
                                .setAmount(amount)
                                .build();

                        // 需要Currency对象，这里使用默认值
                        com.cmex.bolt.domain.Currency currency = com.cmex.bolt.domain.Currency.builder()
                                .id(currencyId)
                                .name("UNKNOWN")
                                .precision(8)
                                .build();

                        transfer.writeIncreaseRequest(request, currency, buffer);
                    }
                    case "DECREASE" -> {
                        int accountId = payloadNode.get("accountId").asInt();
                        int currencyId = payloadNode.get("currencyId").asInt();
                        String amount = payloadNode.get("amount").asText();

                        com.cmex.bolt.Envoy.DecreaseRequest request = com.cmex.bolt.Envoy.DecreaseRequest.newBuilder()
                                .setAccountId(accountId)
                                .setAmount(amount)
                                .build();

                        com.cmex.bolt.domain.Currency currency = com.cmex.bolt.domain.Currency.builder()
                                .id(currencyId)
                                .name("UNKNOWN")
                                .precision(8)
                                .build();

                        transfer.writeDecreaseRequest(request, currency, buffer);
                    }
                    case "PLACE_ORDER" -> {
                        int symbolId = payloadNode.get("symbolId").asInt();
                        int accountId = payloadNode.get("accountId").asInt();
                        String type = payloadNode.get("type").asText();
                        String side = payloadNode.get("side").asText();
                        String price = payloadNode.get("price").asText();
                        String quantity = payloadNode.get("quantity").asText();
                        String volume = payloadNode.get("volume").asText();
                        int takerRate = payloadNode.get("takerRate").asInt();
                        int makerRate = payloadNode.get("makerRate").asInt();

                        com.cmex.bolt.Envoy.PlaceOrderRequest request = com.cmex.bolt.Envoy.PlaceOrderRequest.newBuilder()
                                .setSymbolId(symbolId)
                                .setAccountId(accountId)
                                .setType("LIMIT".equals(type) ? com.cmex.bolt.Envoy.Type.LIMIT : com.cmex.bolt.Envoy.Type.MARKET)
                                .setSide("BID".equals(side) ? com.cmex.bolt.Envoy.Side.BID : com.cmex.bolt.Envoy.Side.ASK)
                                .setPrice(price)
                                .setQuantity(quantity)
                                .setVolume(volume)
                                .setTakerRate(takerRate)
                                .setMakerRate(makerRate)
                                .build();

                        // 需要Symbol对象，这里使用默认值
                        com.cmex.bolt.domain.Currency baseCurrency = com.cmex.bolt.domain.Currency.builder()
                                .id(1)
                                .name("BASE")
                                .precision(8)
                                .build();
                        com.cmex.bolt.domain.Currency quoteCurrency = com.cmex.bolt.domain.Currency.builder()
                                .id(2)
                                .name("QUOTE")
                                .precision(8)
                                .build();

                        com.cmex.bolt.domain.Symbol symbol = com.cmex.bolt.domain.Symbol.builder()
                                .id(symbolId)
                                .name("UNKNOWN")
                                .base(baseCurrency)
                                .quote(quoteCurrency)
                                .quoteSettlement(true)
                                .build();

                        transfer.writePlaceOrderRequest(request, symbol, buffer);
                    }
                    case "CANCEL_ORDER" -> {
                        long orderId = payloadNode.get("orderId").asLong();

                        com.cmex.bolt.Envoy.CancelOrderRequest request = com.cmex.bolt.Envoy.CancelOrderRequest.newBuilder()
                                .setOrderId(orderId)
                                .build();

                        transfer.writeCancelOrderRequest(request, buffer);
                    }
                    default -> {
                        log.warn("Unsupported payload type for replay: {}", payloadType);
                        return null;
                    }
                }

                // 将ByteBuf转换为ByteBuffer
                ByteBuffer result = ByteBuffer.allocate(buffer.readableBytes());
                buffer.getBytes(buffer.readerIndex(), result);
                result.flip();

                // 注意：不要释放buffer，它是NexusWrapper中的池化ByteBuf
                return result;

            } catch (Exception e) {
                // 注意：不要释放buffer，它是NexusWrapper中的池化ByteBuf
                throw e;
            }

        } catch (Exception e) {
            log.warn("Failed to convert JSON data to Cap'n Proto: {}", e.getMessage(), e);
            return null;
        }
    }


    /**
     * 将事件发布到目标 RingBuffer，包含ID、partition和eventType信息
     */
    private void publishEventToRingBuffer(ByteBuffer messageBuffer, int combined) {
        targetRingBuffer.publishEvent((wrapper, sequence) -> {
            // 将 ByteBuffer 内容复制到 NexusWrapper 的 ByteBuf
            ByteBuf buffer = wrapper.getBuffer();
            buffer.clear();
            buffer.writeBytes(messageBuffer);
            wrapper.setPartitionByCombined(combined);
            wrapper.setEventType(NexusWrapper.EventType.JOURNAL);
        });
    }


    /**
     * 获取最大消息大小限制
     * 注意：这里返回的是Cap'n Proto消息本身的大小，不包括ID和partition字段
     */
    private int getMaxMessageSize() {
        // 生产环境允许更大的消息
        return config.isProd() ? DEFAULT_MAX_MESSAGE_SIZE * 2 : DEFAULT_MAX_MESSAGE_SIZE;
    }

    /**
     * 获取日志记录间隔
     */
    private int getLogInterval() {
        // 生产环境减少日志频率
        return config.isProd() ? DEFAULT_LOG_INTERVAL * 2 : DEFAULT_LOG_INTERVAL;
    }

}
