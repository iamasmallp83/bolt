package com.cmex.bolt.handler;

import com.cmex.bolt.Nexus;
import com.cmex.bolt.core.BoltConfig;
import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.domain.Transfer;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

@Slf4j
public class JournalHandler implements EventHandler<NexusWrapper>, LifecycleAware {

    private FileChannel journalChannel;
    private final BoltConfig config;
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(20); // 8+8+4 = 20, with some padding
    private final Transfer transfer;

    public JournalHandler(BoltConfig config) {
        this.config = config;
        this.transfer = new Transfer();

        // 如果禁用日志，不创建 journal 文件
        if (!config.enableJournal()) {
            log.info("Journal disabled, skipping journal file initialization");
            this.journalChannel = null;
            return;
        }

        if (config.isMaster()) {

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
    }

    @Override
    public void onEvent(NexusWrapper wrapper, long sequence, boolean endOfBatch) throws Exception {
        try {
            // 如果禁用日志，跳过 journal 写入
            if (!config.enableJournal()) {
                return;
            }

            // 处理 snapshot 事件 - 创建新的 journal 文件
            if (wrapper.isSnapshotEvent()) {
                handleSnapshotEvent(wrapper);
                wrapper.getBuffer().resetReaderIndex();
                return;
            }

            if (wrapper.isSlaveJoined()) {
                copyJournal(wrapper);
                wrapper.getBuffer().resetReaderIndex();
                return;
            }

            // 跳过 journal与内部 事件本身
            if (wrapper.isJournalEvent() || wrapper.isInternalEvent()) {
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
            wrapper.getBuffer().resetReaderIndex();

        } catch (IOException e) {
            log.error("Failed to write to journal", e);
            throw new RuntimeException("Journal write failed", e);
        }
    }

    private void copyJournal(NexusWrapper wrapper) {
        try {
            // 如果禁用日志，跳过复制操作
            if (!config.enableJournal()) {
                return;
            }

            Path currentJournalPath = Path.of(config.journalFilePath());

            if (!Files.exists(currentJournalPath)) {
                log.warn("Current journal file does not exist: {}", currentJournalPath);
                return;
            }

            // 生成带 replication 前缀的文件名
            String replicationFilename = "replication_" + wrapper.getBuffer().readInt() + "_" + currentJournalPath.getFileName().toString();
            Path journalDir = currentJournalPath.getParent();
            Path replicationJournalPath = journalDir.resolve(replicationFilename);
            // 复制文件
            Files.deleteIfExists(replicationJournalPath);
            Files.copy(currentJournalPath, replicationJournalPath);
            log.info("Copied journal file from {} to {}", currentJournalPath, replicationJournalPath);

        } catch (IOException e) {
            log.error("Failed to copy journal file", e);
            throw new RuntimeException("Failed to copy journal file", e);
        }
    }

    /**
     * 处理 snapshot 事件 - 重命名当前 journal 文件并创建新的 journal 文件
     */
    private void handleSnapshotEvent(NexusWrapper wrapper) throws IOException {
        try {
            // 反序列化获取 snapshot 时间戳
            Transfer transfer = new Transfer();
            Nexus.NexusEvent.Reader nexusEvent = transfer.from(wrapper.getBuffer());

            if (nexusEvent.getPayload().which() == Nexus.Payload.Which.SNAPSHOT) {
                Nexus.Snapshot.Reader snapshot = nexusEvent.getPayload().getSnapshot();
                long timestamp = snapshot.getTimestamp();

                // 关闭当前 journal 文件
                if (journalChannel != null) {
                    journalChannel.force(true);
                    journalChannel.close();

                    // 重命名当前 journal 文件为带时间戳的格式
                    renameCurrentJournalFile(timestamp);
                }

                // 创建新的 journal 文件
                createNewJournalFile();

                log.info("Renamed current journal file and created new journal file for snapshot timestamp: {} (format: {})",
                        timestamp, config.isBinary() ? "binary" : "JSON");
            }
        } catch (Exception e) {
            log.error("Failed to handle snapshot event", e);
            throw new IOException("Failed to handle snapshot event", e);
        }
    }

    /**
     * 重命名当前 journal 文件为带时间戳的格式
     */
    private void renameCurrentJournalFile(long timestamp) throws IOException {
        Path currentJournalPath = Path.of(config.journalFilePath());

        if (Files.exists(currentJournalPath)) {
            // 根据配置的格式生成带时间戳的文件名
            String extension = config.isBinary() ? ".data" : ".json";
            String timestampedFilename = String.format("journal_%d%s", timestamp, extension);
            Path journalDir = Path.of(config.journalDir());
            Path timestampedJournalFile = journalDir.resolve(timestampedFilename);

            // 确保目录存在
            Files.createDirectories(journalDir);

            // 重命名文件
            Files.move(currentJournalPath, timestampedJournalFile);
            log.debug("Renamed journal file from {} to {}", currentJournalPath, timestampedJournalFile);
        }
    }

    /**
     * 创建新的 journal 文件（使用默认名称）
     */
    private void createNewJournalFile() throws IOException {
        Path journalPath = Path.of(config.journalFilePath());

        // 确保父目录存在
        Path parentDir = journalPath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // 创建新的 journal 文件（如果不存在）
        if (!Files.exists(journalPath)) {
            Files.createFile(journalPath);
        }

        // 重新打开 journal channel
        this.journalChannel = FileChannel.open(journalPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    /**
     * 优化的二进制模式写入
     */
    private void writeBinaryMode(NexusWrapper wrapper) throws IOException {
        // 计算总长度：消息长度 + 时间戳(8字节) + ID(8字节) + combinedPartitionAndEventType(4字节)
        int messageLength = wrapper.getBuffer().readableBytes();
        int totalLength = messageLength + 20; // 8+8+4 = 20

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

        // 写入消息内容
        ByteBuffer messageBuffer = wrapper.getBuffer().nioBuffer();
        journalChannel.write(messageBuffer);
    }

    /**
     * 优化的JSON模式写入
     */
    private void writeJsonMode(NexusWrapper wrapper) throws IOException {
        String jsonData = convertToJson(wrapper);
        ByteBuffer jsonBuffer = ByteBuffer.wrap(jsonData.getBytes(StandardCharsets.UTF_8));
        journalChannel.write(jsonBuffer);
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
