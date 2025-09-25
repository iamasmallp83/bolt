package com.cmex.bolt.util;

import com.cmex.bolt.domain.Transfer;
import com.cmex.bolt.Nexus;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 工具类：将journal.data文件内容转换为JSON格式输出
 * 支持二进制格式和JSON格式的journal文件
 */
public class BinaryToJsonConverter {

    private final Path path;
    private final Transfer transfer = new Transfer();

    public BinaryToJsonConverter(Path path) {
        this.path = path;
    }

    /**
     * 将journal文件转换为JSON格式
     *
     * @return JSON格式的字符串列表
     */
    public List<String> convert() {
        if (!Files.exists(this.path)) {
            System.err.println("Journal file does not exist: " + path);
            return new ArrayList<>();
        }

        try {
            long fileSize = Files.size(path);
            System.out.println("Converting journal file: " + path + " (size: " + fileSize + " bytes)");

            return convertBinaryJournalToJson(path);
        } catch (Exception e) {
            System.err.println("Failed to convert journal file: " + path + " - " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * 将二进制格式的journal文件转换为JSON
     */
    private List<String> convertBinaryJournalToJson(Path journalPath) {
        List<String> jsonLines = new ArrayList<>();

        try (FileChannel journalChannel = FileChannel.open(journalPath, StandardOpenOption.READ)) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            long totalEvents = 0;

            System.out.println("Converting binary journal to JSON...");

            while (journalChannel.position() < journalChannel.size()) {
                // 读取总长度
                lengthBuffer.clear();
                int bytesRead = journalChannel.read(lengthBuffer);
                if (bytesRead != 4) {
                    System.err.println("Incomplete length header at position " + journalChannel.position() + ", stopping conversion");
                    break;
                }

                lengthBuffer.flip();
                int totalLength = lengthBuffer.getInt();

                // 验证总长度合理性
                if (totalLength <= 20 || totalLength > 1024 * 1024 + 20) {
                    System.err.println("Invalid total length: " + totalLength + ", stopping conversion");
                    break;
                }

                // 读取时间戳（8字节）
                ByteBuffer timestampBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(timestampBuffer);
                if (bytesRead != 8) {
                    System.err.println("Incomplete timestamp data at position " + journalChannel.position() + ", stopping conversion");
                    break;
                }
                timestampBuffer.flip();
                long timestamp = timestampBuffer.getLong();

                // 读取ID（8字节）
                ByteBuffer idBuffer = ByteBuffer.allocate(8);
                bytesRead = journalChannel.read(idBuffer);
                if (bytesRead != 8) {
                    System.err.println("Incomplete ID data at position " + journalChannel.position() + ", stopping conversion");
                    break;
                }
                idBuffer.flip();
                long id = idBuffer.getLong();

                // 读取partition（4字节）
                ByteBuffer partitionBuffer = ByteBuffer.allocate(4);
                bytesRead = journalChannel.read(partitionBuffer);
                if (bytesRead != 4) {
                    System.err.println("Incomplete partition data at position " + journalChannel.position() + ", stopping conversion");
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
                    System.err.println("Incomplete message data at position " + journalChannel.position() + ", stopping conversion");
                    break;
                }

                // 转换为JSON
                String jsonLine = convertBinaryMessageToJson(timestamp, id, partition, messageBuffer);
                if (jsonLine != null) {
                    jsonLines.add(jsonLine);
                    totalEvents++;
                }

                // 每1000条记录输出一次进度
                if (totalEvents % 1000 == 0) {
                    System.out.println("Converted " + totalEvents + " events...");
                }
            }

            System.out.println("Binary journal conversion completed. Total events converted: " + totalEvents);
            return jsonLines;

        } catch (Exception e) {
            System.err.println("Failed to convert binary journal: " + journalPath + " - " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * 将二进制消息转换为JSON格式
     */
    private String convertBinaryMessageToJson(long timestamp, long id, int partition, ByteBuffer messageBuffer) {
        try {
            // 确保ByteBuffer位置正确 - 重置到开始位置
            messageBuffer.rewind();
            
            // 将ByteBuffer转换为ByteBuf进行反序列化
            io.grpc.netty.shaded.io.netty.buffer.ByteBuf buffer =
                    io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator.DEFAULT.buffer(messageBuffer.remaining());
            buffer.writeBytes(messageBuffer);

            // 反序列化Cap'n Proto数据
            Nexus.NexusEvent.Reader nexusEvent = transfer.from(buffer);

            // 构建JSON
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"timestamp\":\"").append(Instant.ofEpochMilli(timestamp).toString()).append("\",");
            json.append("\"id\":").append(id).append(",");
            json.append("\"partition\":").append(partition).append(",");
            json.append("\"data\":");

            // 将Nexus事件转换为JSON
            json.append(convertNexusEventToJson(nexusEvent));
            json.append("}");

            buffer.release();
            return json.toString();

        } catch (Exception e) {
            System.err.println("Failed to convert binary message to JSON - ID: " + id + 
                             ", Partition: " + partition + ", Error: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 将Nexus事件转换为JSON格式
     * 复用JournalHandler中的逻辑
     */
    private String convertNexusEventToJson(Nexus.NexusEvent.Reader nexusEvent) {
        StringBuilder json = new StringBuilder();
        json.append("{");

        Nexus.Payload.Reader payload = nexusEvent.getPayload();
        json.append("\"payloadType\":\"").append(payload.which().toString()).append("\",");
        json.append("\"payload\":");

        switch (payload.which()) {
            case INCREASE -> {
                Nexus.Increase.Reader increase = payload.getIncrease();
                json.append("{");
                json.append("\"accountId\":").append(increase.getAccountId()).append(",");
                json.append("\"currencyId\":").append(increase.getCurrencyId()).append(",");
                json.append("\"amount\":\"").append(increase.getAmount()).append("\"");
                json.append("}");
            }
            case DECREASE -> {
                Nexus.Decrease.Reader decrease = payload.getDecrease();
                json.append("{");
                json.append("\"accountId\":").append(decrease.getAccountId()).append(",");
                json.append("\"currencyId\":").append(decrease.getCurrencyId()).append(",");
                json.append("\"amount\":\"").append(decrease.getAmount()).append("\"");
                json.append("}");
            }
            case PLACE_ORDER -> {
                Nexus.PlaceOrder.Reader placeOrder = payload.getPlaceOrder();
                json.append("{");
                json.append("\"symbolId\":").append(placeOrder.getSymbolId()).append(",");
                json.append("\"accountId\":").append(placeOrder.getAccountId()).append(",");
                json.append("\"type\":\"").append(placeOrder.getType().toString()).append("\",");
                json.append("\"side\":\"").append(placeOrder.getSide().toString()).append("\",");
                json.append("\"price\":\"").append(placeOrder.getPrice()).append("\",");
                json.append("\"quantity\":\"").append(placeOrder.getQuantity()).append("\",");
                json.append("\"volume\":\"").append(placeOrder.getVolume()).append("\",");
                json.append("\"frozen\":\"").append(placeOrder.getFrozen()).append("\",");
                json.append("\"takerRate\":").append(placeOrder.getTakerRate()).append(",");
                json.append("\"makerRate\":").append(placeOrder.getMakerRate());
                json.append("}");
            }
            case CANCEL_ORDER -> {
                Nexus.CancelOrder.Reader cancelOrder = payload.getCancelOrder();
                json.append("{");
                json.append("\"orderId\":").append(cancelOrder.getOrderId());
                json.append("}");
            }
            case INCREASED -> {
                Nexus.Increased.Reader increased = payload.getIncreased();
                json.append("{");
                json.append("\"currencyId\":").append(increased.getCurrencyId()).append(",");
                json.append("\"amount\":\"").append(increased.getAmount()).append("\",");
                json.append("\"available\":\"").append(increased.getAvailable()).append("\",");
                json.append("\"frozen\":\"").append(increased.getFrozen()).append("\"");
                json.append("}");
            }
            case DECREASED -> {
                Nexus.Decreased.Reader decreased = payload.getDecreased();
                json.append("{");
                json.append("\"currencyId\":").append(decreased.getCurrencyId()).append(",");
                json.append("\"amount\":\"").append(decreased.getAmount()).append("\",");
                json.append("\"available\":\"").append(decreased.getAvailable()).append("\",");
                json.append("\"frozen\":\"").append(decreased.getFrozen()).append("\"");
                json.append("}");
            }
            case ORDER_CREATED -> {
                Nexus.OrderCreated.Reader orderCreated = payload.getOrderCreated();
                json.append("{");
                json.append("\"orderId\":").append(orderCreated.getOrderId());
                json.append("}");
            }
            case ORDER_CANCELED -> {
                Nexus.OrderCanceled.Reader orderCanceled = payload.getOrderCanceled();
                json.append("{");
                json.append("\"orderId\":").append(orderCanceled.getOrderId());
                json.append("}");
            }
            case DECREASE_REJECTED -> {
                Nexus.DecreaseRejected.Reader rejected = payload.getDecreaseRejected();
                json.append("{");
                json.append("\"reason\":\"").append(rejected.getReason().toString()).append("\"");
                json.append("}");
            }
            case PLACE_ORDER_REJECTED -> {
                Nexus.PlaceOrderRejected.Reader rejected = payload.getPlaceOrderRejected();
                json.append("{");
                json.append("\"reason\":\"").append(rejected.getReason().toString()).append("\"");
                json.append("}");
            }
            case CANCEL_ORDER_REJECTED -> {
                Nexus.CancelOrderRejected.Reader rejected = payload.getCancelOrderRejected();
                json.append("{");
                json.append("\"reason\":\"").append(rejected.getReason().toString()).append("\"");
                json.append("}");
            }
            case UNFREEZE -> {
                Nexus.Unfreeze.Reader unfreeze = payload.getUnfreeze();
                json.append("{");
                json.append("\"accountId\":").append(unfreeze.getAccountId()).append(",");
                json.append("\"currencyId\":").append(unfreeze.getCurrencyId()).append(",");
                json.append("\"amount\":\"").append(unfreeze.getAmount()).append("\"");
                json.append("}");
            }
            case CLEAR -> {
                Nexus.Clear.Reader clear = payload.getClear();
                json.append("{");
                json.append("\"accountId\":").append(clear.getAccountId()).append(",");
                json.append("\"payCurrencyId\":").append(clear.getPayCurrencyId()).append(",");
                json.append("\"payAmount\":\"").append(clear.getPayAmount()).append("\",");
                json.append("\"refundAmount\":\"").append(clear.getRefundAmount()).append("\",");
                json.append("\"incomeCurrencyId\":").append(clear.getIncomeCurrencyId()).append(",");
                json.append("\"incomeAmount\":\"").append(clear.getIncomeAmount()).append("\"");
                json.append("}");
            }
            default -> {
                json.append("{\"unknown\":\"Unknown payload type: ").append(payload.which().toString()).append("\"}");
            }
        }

        json.append("}");
        return json.toString();
    }

    /**
     * 将转换结果保存到文件
     *
     * @param jsonLines  JSON字符串列表
     * @param outputPath 输出文件路径
     */
    public void saveToFile(List<String> jsonLines, String outputPath) {
        saveToFile(jsonLines, Path.of(outputPath));
    }

    /**
     * 将转换结果保存到文件
     *
     * @param jsonLines  JSON字符串列表
     * @param outputPath 输出文件路径
     */
    public void saveToFile(List<String> jsonLines, Path outputPath) {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            for (String jsonLine : jsonLines) {
                writer.println(jsonLine);
            }
            System.out.println("JSON output saved to: " + outputPath + " (" + jsonLines.size() + " lines)");
        } catch (Exception e) {
            System.err.println("Failed to save JSON output to: " + outputPath + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 打印转换结果到控制台
     *
     * @param jsonLines JSON字符串列表
     */
    public void printToConsole(List<String> jsonLines) {
        for (String jsonLine : jsonLines) {
            System.out.println(jsonLine);
        }
        System.out.println("Printed " + jsonLines.size() + " JSON lines to console");
    }

    /**
     * 命令行入口点
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java JournalToJsonConverter <journal_file_path> [output_file_path] [--binary]");
            System.err.println("  journal_file_path: Path to the journal file");
            System.err.println("  output_file_path: Optional output file path (default: print to console)");
            System.err.println("  --binary: Force binary mode (default: auto-detect from file extension)");
            System.exit(1);
        }

        String journalFilePath = args[0];
        String outputFilePath = args.length > 1 ? args[1] : null;

        BinaryToJsonConverter converter = new BinaryToJsonConverter(Path.of(journalFilePath));

        try {
            List<String> jsonLines = converter.convert();

            if (outputFilePath != null) {
                converter.saveToFile(jsonLines, outputFilePath);
            } else {
                converter.printToConsole(jsonLines);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
