package com.cmex.bolt.handler;

import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
public class JournalHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private final FileChannel journalChannel;
    private final BoltConfig config;
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(20); // 8+8+4 = 20, with some padding

    public JournalHandler(BoltConfig config) {
        this.config = config;

        // 如果禁用日志，不创建 journal 文件
        if (!config.enableJournal()) {
            log.info("Journal disabled, skipping journal file initialization");
            this.journalChannel = null;
            return;
        }

        try {
            // 根据isBinary标志添加相应的文件后缀
            Path journalPath = Path.of(config.journalFilePath());

            // 确保父目录存在
            Path parentDir = journalPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
                log.info("Created journal directory: {}", parentDir);
            }

            this.journalChannel = FileChannel.open(journalPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
            log.info("JournalHandler initialized with file: {}, isBinary: {}", config.journalFilePath(), config.isBinary());
        } catch (IOException e) {
            log.error("Failed to initialize journal file: {}", config.journalFilePath(), e);
            throw new RuntimeException("Failed to initialize journal file: " + config.journalFilePath(), e);
        }
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        try {
            // 如果禁用日志，跳过 journal 写入
            if (!config.enableJournal() || wrapper.isJournalEvent()) {
                return;
            }

            if (config.isBinary()) {
                writeBinaryMode(wrapper);
            } else {
                writeJsonMode(wrapper);
            }

            // 强制刷新到磁盘（可选，根据性能需求调整）
            if (endOfBatch) {
                journalChannel.force(false);
            }

        } catch (IOException e) {
            log.error("Failed to write to journal", e);
            throw new RuntimeException("Journal write failed", e);
        }
    }

    /**
     * 优化的二进制模式写入
     */
    private void writeBinaryMode(NexusWrapper wrapper) throws IOException {
        // 计算总长度：消息长度 + 时间戳(8字节) + ID(8字节) + combinedPartitionAndEventType(4字节)
        int totalLength = 64 + 20; // 8+8+4 = 20

        // 写入总长度（4字节）
        lengthBuffer.clear();
        lengthBuffer.putInt(totalLength);
        lengthBuffer.flip();
        journalChannel.write(lengthBuffer);

        // 使用单个headerBuffer写入时间戳、ID和combinedPartitionAndEventType，减少系统调用
        headerBuffer.clear();
        headerBuffer.putLong(System.currentTimeMillis());
        headerBuffer.putLong(wrapper.getId());
        headerBuffer.putInt(wrapper.getCombinedPartitionAndEventType());
        headerBuffer.flip();
        journalChannel.write(headerBuffer);
    }

    /**
     * 优化的JSON模式写入
     */
    private void writeJsonMode(NexusWrapper wrapper) throws IOException {
        String jsonData = convertToJson(wrapper);
        ByteBuffer jsonBuffer = ByteBuffer.wrap(jsonData.getBytes(StandardCharsets.UTF_8));
        journalChannel.write(jsonBuffer);
        wrapper.getBuffer().resetReaderIndex();
    }

    /**
     * 优化的Cap'n Proto数据转换为JSON格式
     * 反序列化Cap'n Proto数据并转换为结构化的JSON
     */
    private String convertToJson(NexusWrapper wrapper) {
        try {
            // 反序列化Cap'n Proto数据
            com.cmex.bolt.domain.Transfer transfer = new com.cmex.bolt.domain.Transfer();
            com.cmex.bolt.Nexus.NexusEvent.Reader nexusEvent = transfer.from(wrapper.getBuffer());

            // 使用StringBuilder预分配容量，减少内存重新分配
            StringBuilder json = new StringBuilder(512);
            json.append("{\"timestamp\":\"").append(Instant.now().toString())
                    .append("\",\"id\":").append(wrapper.getId())
                    .append(",\"partition\":").append(wrapper.getPartition())
                    .append(",\"eventType\":").append(wrapper.getEventType().ordinal())
                    .append(",\"data\":");

            // 将Nexus事件转换为JSON
            json.append(convertNexusEventToJson(nexusEvent));
            json.append("}\n");

            return json.toString();
        } catch (Exception e) {
            log.error("Failed to convert to JSON", e);
            // 如果转换失败，返回一个基本的错误JSON
            return "{\"error\":\"Failed to convert to JSON\",\"timestamp\":\"" +
                    Instant.now().toString() + "\",\"message\":\"" + e.getMessage() + "\"}";
        }
    }

    /**
     * 优化的Nexus事件转换为JSON格式
     */
    private String convertNexusEventToJson(com.cmex.bolt.Nexus.NexusEvent.Reader nexusEvent) {
        // 预分配容量，减少内存重新分配
        StringBuilder json = new StringBuilder(256);
        json.append("{\"payloadType\":\"").append(nexusEvent.getPayload().which().toString())
                .append("\",\"payload\":");

        com.cmex.bolt.Nexus.Payload.Reader payload = nexusEvent.getPayload();
        switch (payload.which()) {
            case INCREASE -> {
                com.cmex.bolt.Nexus.Increase.Reader increase = payload.getIncrease();
                json.append("{\"accountId\":").append(increase.getAccountId())
                        .append(",\"currencyId\":").append(increase.getCurrencyId())
                        .append(",\"amount\":\"").append(increase.getAmount()).append("\"}");
            }
            case DECREASE -> {
                com.cmex.bolt.Nexus.Decrease.Reader decrease = payload.getDecrease();
                json.append("{\"accountId\":").append(decrease.getAccountId())
                        .append(",\"currencyId\":").append(decrease.getCurrencyId())
                        .append(",\"amount\":\"").append(decrease.getAmount()).append("\"}");
            }
            case PLACE_ORDER -> {
                com.cmex.bolt.Nexus.PlaceOrder.Reader placeOrder = payload.getPlaceOrder();
                json.append("{\"symbolId\":").append(placeOrder.getSymbolId())
                        .append(",\"accountId\":").append(placeOrder.getAccountId())
                        .append(",\"type\":\"").append(placeOrder.getType().toString())
                        .append("\",\"side\":\"").append(placeOrder.getSide().toString())
                        .append("\",\"price\":\"").append(placeOrder.getPrice())
                        .append("\",\"quantity\":\"").append(placeOrder.getQuantity())
                        .append("\",\"volume\":\"").append(placeOrder.getVolume())
                        .append("\",\"frozen\":\"").append(placeOrder.getFrozen())
                        .append("\",\"takerRate\":").append(placeOrder.getTakerRate())
                        .append(",\"makerRate\":").append(placeOrder.getMakerRate()).append("}");
            }
            case CANCEL_ORDER -> {
                com.cmex.bolt.Nexus.CancelOrder.Reader cancelOrder = payload.getCancelOrder();
                json.append("{\"orderId\":").append(cancelOrder.getOrderId()).append("}");
            }
            case INCREASED -> {
                com.cmex.bolt.Nexus.Increased.Reader increased = payload.getIncreased();
                json.append("{\"currencyId\":").append(increased.getCurrencyId())
                        .append(",\"amount\":\"").append(increased.getAmount())
                        .append("\",\"available\":\"").append(increased.getAvailable())
                        .append("\",\"frozen\":\"").append(increased.getFrozen()).append("\"}");
            }
            case DECREASED -> {
                com.cmex.bolt.Nexus.Decreased.Reader decreased = payload.getDecreased();
                json.append("{\"currencyId\":").append(decreased.getCurrencyId())
                        .append(",\"amount\":\"").append(decreased.getAmount())
                        .append("\",\"available\":\"").append(decreased.getAvailable())
                        .append("\",\"frozen\":\"").append(decreased.getFrozen()).append("\"}");
            }
            case ORDER_CREATED -> {
                com.cmex.bolt.Nexus.OrderCreated.Reader orderCreated = payload.getOrderCreated();
                json.append("{\"orderId\":").append(orderCreated.getOrderId()).append("}");
            }
            case ORDER_CANCELED -> {
                com.cmex.bolt.Nexus.OrderCanceled.Reader orderCanceled = payload.getOrderCanceled();
                json.append("{\"orderId\":").append(orderCanceled.getOrderId()).append("}");
            }
            case DECREASE_REJECTED -> {
                com.cmex.bolt.Nexus.DecreaseRejected.Reader rejected = payload.getDecreaseRejected();
                json.append("{\"reason\":\"").append(rejected.getReason().toString()).append("\"}");
            }
            case PLACE_ORDER_REJECTED -> {
                com.cmex.bolt.Nexus.PlaceOrderRejected.Reader rejected = payload.getPlaceOrderRejected();
                json.append("{\"reason\":\"").append(rejected.getReason().toString()).append("\"}");
            }
            case CANCEL_ORDER_REJECTED -> {
                com.cmex.bolt.Nexus.CancelOrderRejected.Reader rejected = payload.getCancelOrderRejected();
                json.append("{\"reason\":\"").append(rejected.getReason().toString()).append("\"}");
            }
            case UNFREEZE -> {
                com.cmex.bolt.Nexus.Unfreeze.Reader unfreeze = payload.getUnfreeze();
                json.append("{\"accountId\":").append(unfreeze.getAccountId())
                        .append(",\"currencyId\":").append(unfreeze.getCurrencyId())
                        .append(",\"amount\":\"").append(unfreeze.getAmount()).append("\"}");
            }
            case CLEAR -> {
                com.cmex.bolt.Nexus.Clear.Reader clear = payload.getClear();
                json.append("{\"accountId\":").append(clear.getAccountId())
                        .append(",\"payCurrencyId\":").append(clear.getPayCurrencyId())
                        .append(",\"payAmount\":\"").append(clear.getPayAmount())
                        .append("\",\"refundAmount\":\"").append(clear.getRefundAmount())
                        .append("\",\"incomeCurrencyId\":").append(clear.getIncomeCurrencyId())
                        .append(",\"incomeAmount\":\"").append(clear.getIncomeAmount()).append("\"}");
            }
            default -> {
                json.append("{\"unknown\":\"Unknown payload type: ").append(payload.which().toString()).append("\"}");
            }
        }

        json.append("}");
        return json.toString();
    }

    @Override
    public void onStart() {
        final Thread currentThread = Thread.currentThread();
        currentThread.setName(JournalHandler.class.getSimpleName() + "-thread");
        log.info("JournalHandler started");
    }

    @Override
    public void onShutdown() {
        try {
            if (journalChannel != null) {
                journalChannel.force(true); // 强制刷新到磁盘
                journalChannel.close();
            }
            log.info("JournalHandler shutdown completed");
        } catch (IOException e) {
            log.error("Error during JournalHandler shutdown", e);
        }
    }
}
