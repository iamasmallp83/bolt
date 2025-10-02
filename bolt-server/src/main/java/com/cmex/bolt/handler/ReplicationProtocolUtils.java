package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationProto;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

/**
 * 复制协议处理器 - 基于 Protocol Buffers
 * 处理注册、心跳、业务、确认4种类型的消息
 */
@Slf4j
public class ReplicationProtocolUtils {

    // 协议常量
    public static final int MAGIC_NUMBER = 0x424F4C54; // "BOLT"
    public static final int VERSION = 1;
    
    /**
     * 编码业务消息
     */
    public static ByteBuf encodeBusinessMessage(NexusWrapper wrapper, long sequence) {
        try {
            // 创建业务消息
            ReplicationProto.BusinessMessage.Builder businessBuilder = ReplicationProto.BusinessMessage.newBuilder()
                    .setSequence(sequence)
                    .setPartition(wrapper.getPartition())
                    .setEventType(wrapper.getEventType().getValue())
                    .setTimestamp(System.currentTimeMillis());
            
            int readableBytes = wrapper.getBuffer().readableBytes();

            if (readableBytes == 0) {
                log.warn("Warning: NexusWrapper buffer has no readable bytes - sequence: {}", sequence);
            }

            byte[] data = new byte[readableBytes];
            wrapper.getBuffer().readBytes(data);
            
            // 恢复原始状态
            businessBuilder.setData(com.google.protobuf.ByteString.copyFrom(data));
            
            ReplicationProto.BusinessMessage businessMessage = businessBuilder.build();
            
            // 创建协议头部
            ReplicationProto.ProtocolHeader header = ReplicationProto.ProtocolHeader.newBuilder()
                    .setMagicNumber(MAGIC_NUMBER)
                    .setVersion(VERSION)
                    .setMessageType(ReplicationProto.MessageType.BUSINESS)
                    .setSequence(sequence)
                    .setDataLength(businessMessage.getSerializedSize())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // 创建主消息
            ReplicationProto.ReplicationMessage message = ReplicationProto.ReplicationMessage.newBuilder()
                    .setHeader(header)
                    .setBusiness(businessMessage)
                    .build();
            
            // 序列化为字节数组
            byte[] messageBytes = message.toByteArray();
            
            // 创建 Netty ByteBuf
            ByteBuf buffer = Unpooled.directBuffer(messageBytes.length);
            buffer.writeBytes(messageBytes);
            
            log.debug("Encoded business message - sequence: {}, size: {}", sequence, messageBytes.length);
            return buffer;
            
        } catch (Exception e) {
            log.error("Failed to encode business message - sequence: {}", sequence, e);
            throw new RuntimeException("Failed to encode business message", e);
        }
    }
    
    /**
     * 编码确认消息
     */
    public static ByteBuf encodeConfirmationMessage(long sequence, int nodeId, boolean success, String errorMessage) {
        try {
            // 创建确认消息
            ReplicationProto.ConfirmationMessage.Builder confirmationBuilder = ReplicationProto.ConfirmationMessage.newBuilder()
                    .setSequence(sequence)
                    .setNodeId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .setSuccess(success);
            
            if (errorMessage != null) {
                confirmationBuilder.setErrorMessage(errorMessage);
            }
            
            ReplicationProto.ConfirmationMessage confirmationMessage = confirmationBuilder.build();
            
            // 创建协议头部
            ReplicationProto.ProtocolHeader header = ReplicationProto.ProtocolHeader.newBuilder()
                    .setMagicNumber(MAGIC_NUMBER)
                    .setVersion(VERSION)
                    .setMessageType(ReplicationProto.MessageType.CONFIRMATION)
                    .setSequence(sequence)
                    .setDataLength(confirmationMessage.getSerializedSize())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // 创建主消息
            ReplicationProto.ReplicationMessage message = ReplicationProto.ReplicationMessage.newBuilder()
                    .setHeader(header)
                    .setConfirmation(confirmationMessage)
                    .build();
            
            // 序列化为字节数组
            byte[] messageBytes = message.toByteArray();
            
            // 创建 Netty ByteBuf
            ByteBuf buffer = Unpooled.directBuffer(messageBytes.length);
            buffer.writeBytes(messageBytes);
            
            log.debug("Encoded confirmation message - sequence: {}, nodeId: {}", sequence, nodeId);
            return buffer;
            
        } catch (Exception e) {
            log.error("Failed to encode confirmation message - sequence: {}", sequence, e);
            throw new RuntimeException("Failed to encode confirmation message", e);
        }
    }
    
    /**
     * 编码注册消息
     */
    public static ByteBuf encodeRegisterMessage(int nodeId, String host, int port, int replicationPort,  
                                              ReplicationProto.NodeType nodeType) {
        try {
            // 创建注册消息
            ReplicationProto.RegisterMessage registerMessage = ReplicationProto.RegisterMessage.newBuilder()
                    .setNodeId(nodeId)
                    .setNodeType(nodeType)
                    .setHost(host)
                    .setPort(port)
                    .setReplicationPort(replicationPort)
                    .build();
            
            // 创建协议头部
            ReplicationProto.ProtocolHeader header = ReplicationProto.ProtocolHeader.newBuilder()
                    .setMagicNumber(MAGIC_NUMBER)
                    .setVersion(VERSION)
                    .setMessageType(ReplicationProto.MessageType.REGISTER)
                    .setSequence(0) // 注册消息序列号为0
                    .setDataLength(registerMessage.getSerializedSize())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // 创建主消息
            ReplicationProto.ReplicationMessage message = ReplicationProto.ReplicationMessage.newBuilder()
                    .setHeader(header)
                    .setRegister(registerMessage)
                    .build();
            
            // 序列化为字节数组
            byte[] messageBytes = message.toByteArray();
            
            // 创建 Netty ByteBuf
            ByteBuf buffer = Unpooled.directBuffer(messageBytes.length);
            buffer.writeBytes(messageBytes);
            
            log.debug("Encoded register message - nodeId: {}, host: {}, port: {}", nodeId, host, port);
            return buffer;
            
        } catch (Exception e) {
            log.error("Failed to encode register message - nodeId: {}", nodeId, e);
            throw new RuntimeException("Failed to encode register message", e);
        }
    }
    
    /**
     * 编码心跳消息
     */
    public static ByteBuf encodeHeartbeatMessage(int nodeId, int sequence) {
        try {
            // 创建心跳消息
            ReplicationProto.HeartbeatMessage heartbeatMessage = ReplicationProto.HeartbeatMessage.newBuilder()
                    .setNodeId(nodeId)
                    .setTimestamp(System.currentTimeMillis())
                    .setSequence(sequence)
                    .build();
            
            // 创建协议头部
            ReplicationProto.ProtocolHeader header = ReplicationProto.ProtocolHeader.newBuilder()
                    .setMagicNumber(MAGIC_NUMBER)
                    .setVersion(VERSION)
                    .setMessageType(ReplicationProto.MessageType.HEARTBEAT)
                    .setSequence(sequence)
                    .setDataLength(heartbeatMessage.getSerializedSize())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            
            // 创建主消息
            ReplicationProto.ReplicationMessage message = ReplicationProto.ReplicationMessage.newBuilder()
                    .setHeader(header)
                    .setHeartbeat(heartbeatMessage)
                    .build();
            
            // 序列化为字节数组
            byte[] messageBytes = message.toByteArray();
            
            // 创建 Netty ByteBuf
            ByteBuf buffer = Unpooled.directBuffer(messageBytes.length);
            buffer.writeBytes(messageBytes);
            
            log.debug("Encoded heartbeat message - nodeId: {}, sequence: {}", nodeId, sequence);
            return buffer;
            
        } catch (Exception e) {
            log.error("Failed to encode heartbeat message - nodeId: {}", nodeId, e);
            throw new RuntimeException("Failed to encode heartbeat message", e);
        }
    }
    
    /**
     * 解码消息
     */
    public static ReplicationProto.ReplicationMessage decodeMessage(ByteBuf buffer) {
        try {
            // 读取消息长度
            int messageLength = buffer.readableBytes();
            byte[] messageBytes = new byte[messageLength];
            buffer.readBytes(messageBytes);
            
            // 解析 Protocol Buffers 消息
            ReplicationProto.ReplicationMessage message = ReplicationProto.ReplicationMessage.parseFrom(messageBytes);
            
            // 验证协议头部
            ReplicationProto.ProtocolHeader header = message.getHeader();
            if (header.getMagicNumber() != MAGIC_NUMBER) {
                throw new IllegalArgumentException("Invalid magic number: " + header.getMagicNumber());
            }
            
            if (header.getVersion() != VERSION) {
                throw new IllegalArgumentException("Unsupported version: " + header.getVersion());
            }
            
            log.debug("Decoded message - type: {}, sequence: {}", header.getMessageType(), header.getSequence());
            return message;
            
        } catch (Exception e) {
            log.error("Failed to decode message", e);
            throw new RuntimeException("Failed to decode message", e);
        }
    }
    
    /**
     * 验证消息完整性
     */
    public static boolean validateMessage(ReplicationProto.ReplicationMessage message) {
        ReplicationProto.ProtocolHeader header = message.getHeader();
        
        // 验证 Magic Number
        if (header.getMagicNumber() != MAGIC_NUMBER) {
            log.error("Invalid magic number: {}", header.getMagicNumber());
            return false;
        }
        
        // 验证版本
        if (header.getVersion() != VERSION) {
            log.error("Unsupported version: {}", header.getVersion());
            return false;
        }
        
        // 验证时间戳（不能是未来时间）
        long currentTime = System.currentTimeMillis();
        if (header.getTimestamp() > currentTime + 60000) { // 允许1分钟的时钟偏差
            log.error("Invalid timestamp: {}, current: {}", header.getTimestamp(), currentTime);
            return false;
        }
        
        return true;
    }
}
