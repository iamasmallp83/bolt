package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 简化的主从复制协议
 * 直接使用NexusWrapper作为消息体，减少协议开销
 */
@Slf4j
public class ReplicationProtocol {
    
    // 协议常量
    private static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    private static final int VERSION = 2; // 新版本协议
    
    // 消息类型
    public static final int MESSAGE_TYPE_NEXUS_WRAPPER = 1; // NexusWrapper消息
    public static final int MESSAGE_TYPE_CONFIRMATION = 2;  // 确认消息
    public static final int MESSAGE_TYPE_HEARTBEAT = 3;     // 心跳消息
    
    /**
     * 序列化NexusWrapper为ByteBuf
     */
    public static ByteBuf serializeNexusWrapper(NexusWrapper wrapper) {
        // 计算总长度：协议头(12) + NexusWrapper头(16) + Cap'n Proto数据
        int capnProtoLength = wrapper.getBuffer().readableBytes();
        int totalLength = 12 + 16 + capnProtoLength;
        
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);
        
        // 写入协议头
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeInt(VERSION);
        buffer.writeInt(MESSAGE_TYPE_NEXUS_WRAPPER);
        
        // 写入NexusWrapper头
        buffer.writeLong(wrapper.getId());
        buffer.writeInt(wrapper.getCombinedPartitionAndEventType());
        buffer.writeInt(capnProtoLength);
        
        // 写入Cap'n Proto数据
        if (capnProtoLength > 0) {
            buffer.writeBytes(wrapper.getBuffer(), wrapper.getBuffer().readerIndex(), capnProtoLength);
        }
        
        return buffer;
    }
    
    /**
     * 反序列化ByteBuf为NexusWrapper
     */
    public static NexusWrapper deserializeNexusWrapper(ByteBuf buffer) throws IOException {
        // 验证协议头
        int magic = buffer.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }
        
        int version = buffer.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        int messageType = buffer.readInt();
        if (messageType != MESSAGE_TYPE_NEXUS_WRAPPER) {
            throw new IOException("Invalid message type: " + messageType);
        }
        
        // 读取NexusWrapper头
        long id = buffer.readLong();
        int combinedPartitionAndEventType = buffer.readInt();
        int capnProtoLength = buffer.readInt();
        
        // 创建NexusWrapper
        NexusWrapper wrapper = new NexusWrapper(PooledByteBufAllocator.DEFAULT, capnProtoLength);
        wrapper.setId(id);
        wrapper.setPartitionByCombined(combinedPartitionAndEventType);
        wrapper.setEventType(NexusWrapper.EventType.SLAVE_REPLAY);
        
        // 读取Cap'n Proto数据
        if (capnProtoLength > 0) {
            wrapper.getBuffer().writeBytes(buffer, capnProtoLength);
        }
        
        return wrapper;
    }
    
    /**
     * 序列化确认消息
     */
    public static ByteBuf serializeConfirmation(long sequence, String slaveNodeId) {
        // 计算总长度：协议头(12) + sequence(8) + slaveNodeId长度(4) + slaveNodeId数据
        byte[] slaveNodeIdBytes = slaveNodeId.getBytes();
        int totalLength = 12 + 8 + 4 + slaveNodeIdBytes.length;
        
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);
        
        // 写入协议头
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeInt(VERSION);
        buffer.writeInt(MESSAGE_TYPE_CONFIRMATION);
        
        // 写入确认数据
        buffer.writeLong(sequence);
        buffer.writeInt(slaveNodeIdBytes.length);
        buffer.writeBytes(slaveNodeIdBytes);
        
        return buffer;
    }
    
    /**
     * 反序列化确认消息
     */
    public static ConfirmationMessage deserializeConfirmation(ByteBuf buffer) throws IOException {
        // 验证协议头
        int magic = buffer.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }
        
        int version = buffer.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        int messageType = buffer.readInt();
        if (messageType != MESSAGE_TYPE_CONFIRMATION) {
            throw new IOException("Invalid message type: " + messageType);
        }
        
        // 读取确认数据
        long sequence = buffer.readLong();
        int slaveNodeIdLength = buffer.readInt();
        byte[] slaveNodeIdBytes = new byte[slaveNodeIdLength];
        buffer.readBytes(slaveNodeIdBytes);
        String slaveNodeId = new String(slaveNodeIdBytes);
        
        return new ConfirmationMessage(sequence, slaveNodeId);
    }
    
    /**
     * 序列化心跳消息
     */
    public static ByteBuf serializeHeartbeat(long timestamp) {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(20);
        
        // 写入协议头
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeInt(VERSION);
        buffer.writeInt(MESSAGE_TYPE_HEARTBEAT);
        
        // 写入时间戳
        buffer.writeLong(timestamp);
        
        return buffer;
    }
    
    /**
     * 反序列化心跳消息
     */
    public static long deserializeHeartbeat(ByteBuf buffer) throws IOException {
        // 验证协议头
        int magic = buffer.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Invalid magic number: " + Integer.toHexString(magic));
        }
        
        int version = buffer.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        
        int messageType = buffer.readInt();
        if (messageType != MESSAGE_TYPE_HEARTBEAT) {
            throw new IOException("Invalid message type: " + messageType);
        }
        
        // 读取时间戳
        return buffer.readLong();
    }
    
    /**
     * 确认消息
     */
    public static class ConfirmationMessage {
        private final long sequence;
        private final String slaveNodeId;
        
        public ConfirmationMessage(long sequence, String slaveNodeId) {
            this.sequence = sequence;
            this.slaveNodeId = slaveNodeId;
        }
        
        public long getSequence() {
            return sequence;
        }
        
        public String getSlaveNodeId() {
            return slaveNodeId;
        }
    }
}
