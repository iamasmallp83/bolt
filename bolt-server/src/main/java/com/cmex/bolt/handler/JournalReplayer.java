package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
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
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
        long startTime = System.currentTimeMillis();

        try {
            Path path = Path.of(config.journalFilePath());

            if (!Files.exists(path)) {
                log.warn("Journal file does not exist: {}", path);
                return 0;
            }

            long fileSize = Files.size(path);
            log.info("Starting journal replay: file={}, size={} bytes, format={}",
                    path, fileSize, config.isBinary() ? "binary" : "JSON");

            long replayedEvents;
            if (config.isBinary()) {
                replayedEvents = replayFromBinaryJournal(path);
            } else {
                replayedEvents = replayFromJsonJournal(path);
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
     * 从二进制格式的 journal 文件重放事件
     *
     * @param journalFilePath journal 文件路径
     * @return 重放的事件数量
     */
    private long replayFromBinaryJournal(Path journalFilePath) {
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
                if (totalLength <= 20 || totalLength > maxMessageSize + 20) { // 20 = 8(timestamp) + 8(ID) + 4(partition)
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

                // 读取ID（8字节）
                ByteBuffer idBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(idBuffer);
                if (bytesRead != 8) {
                    log.warn("Incomplete ID data at position {}, stopping replay", journalChannel.position());
                    break;
                }
                idBuffer.flip();
                long id = idBuffer.getLong();

                // 读取partition（4字节）
                ByteBuffer partitionBuffer = ByteBuffer.allocate(4);
                bytesRead = journalChannel.read(partitionBuffer);
                if (bytesRead != 4) {
                    log.warn("Incomplete partition data at position {}, stopping replay", journalChannel.position());
                    break;
                }
                partitionBuffer.flip();
                int partition = partitionBuffer.getInt();

                // 计算消息内容长度
                int messageLength = totalLength - 20; // 总长度 - 时间戳(8) - ID(8) - partition(4)

                // 读取消息内容
                ByteBuffer messageBuffer = ByteBuffer.allocate(messageLength);
                bytesRead = journalChannel.read(messageBuffer);
                if (bytesRead != messageLength) {
                    log.warn("Incomplete message data at position {}, stopping replay", journalChannel.position());
                    break;
                }

                // 发布事件到 RingBuffer，包含ID和partition信息
                messageBuffer.flip();
                publishEventToRingBuffer(messageBuffer, partition);
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

                    // 设置wrapper的元数据
                    wrapper.setId(-1);
                    wrapper.setPartition(rootNode.get("partition").asInt());
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
                        buffer.release();
                        return null;
                    }
                }

                // 将ByteBuf转换为ByteBuffer
                ByteBuffer result = ByteBuffer.allocate(buffer.readableBytes());
                buffer.getBytes(buffer.readerIndex(), result);
                result.flip();

                buffer.release(); // 释放ByteBuf
                return result;

            } catch (Exception e) {
                buffer.release(); // 确保释放ByteBuf
                throw e;
            }

        } catch (Exception e) {
            log.warn("Failed to convert JSON data to Cap'n Proto: {}", e.getMessage(), e);
            return null;
        }
    }


    /**
     * 将事件发布到目标 RingBuffer，包含ID和partition信息
     */
    private void publishEventToRingBuffer(ByteBuffer messageBuffer, int partition) {
        targetRingBuffer.publishEvent((wrapper, sequence) -> {
            // 将 ByteBuffer 内容复制到 NexusWrapper 的 ByteBuf
            ByteBuf buffer = wrapper.getBuffer();
            buffer.clear();
            buffer.writeBytes(messageBuffer);

            wrapper.setId(-1);
            wrapper.setPartition(partition);
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
