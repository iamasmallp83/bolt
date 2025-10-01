package com.cmex.bolt.handler;

import com.cmex.bolt.core.NexusWrapper;
import com.cmex.bolt.replication.ReplicationProto;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 复制协议处理器测试
 */
public class ReplicationProtocolHandlerTest {

    @Test
    void testEncodeDecodeBusinessMessage() {
        // 创建测试数据
        NexusWrapper wrapper = new NexusWrapper(io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator.DEFAULT, 512);
        wrapper.setId(12345L);
        wrapper.setPartition(1);
        wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        
        // 添加一些测试数据
        byte[] testData = "test business data".getBytes();
        wrapper.getBuffer().writeBytes(testData);
        
        long sequence = 100L;
        
        // 编码消息
        ByteBuf encodedMessage = ReplicationProtocolUtils.encodeBusinessMessage(wrapper, sequence);
        assertNotNull(encodedMessage);
        assertTrue(encodedMessage.readableBytes() > 0);
        
        // 解码消息
        ReplicationProto.ReplicationMessage decodedMessage = ReplicationProtocolUtils.decodeMessage(encodedMessage);
        assertNotNull(decodedMessage);
        
        // 验证消息内容
        ReplicationProto.ProtocolHeader header = decodedMessage.getHeader();
        assertEquals(ReplicationProtocolUtils.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(ReplicationProtocolUtils.VERSION, header.getVersion());
        assertEquals(ReplicationProto.MessageType.BUSINESS, header.getMessageType());
        assertEquals(sequence, header.getSequence());
        
        ReplicationProto.BusinessMessage businessMessage = decodedMessage.getBusiness();
        assertEquals(sequence, businessMessage.getSequence());
        assertEquals(1, businessMessage.getPartition());
        assertEquals(NexusWrapper.EventType.BUSINESS.getValue(), businessMessage.getEventType());
        
        // 验证数据内容
        byte[] decodedData = businessMessage.getData().toByteArray();
        assertArrayEquals(testData, decodedData);
        
        // 验证消息完整性
        assertTrue(ReplicationProtocolUtils.validateMessage(decodedMessage));
        
        encodedMessage.release();
    }
    
    @Test
    void testEncodeDecodeConfirmationMessage() {
        long sequence = 200L;
        String nodeId = "slave-001";
        boolean success = true;
        String errorMessage = null;
        
        // 编码确认消息
        ByteBuf encodedMessage = ReplicationProtocolUtils.encodeConfirmationMessage(sequence, nodeId, success, errorMessage);
        assertNotNull(encodedMessage);
        assertTrue(encodedMessage.readableBytes() > 0);
        
        // 解码消息
        ReplicationProto.ReplicationMessage decodedMessage = ReplicationProtocolUtils.decodeMessage(encodedMessage);
        assertNotNull(decodedMessage);
        
        // 验证消息内容
        ReplicationProto.ProtocolHeader header = decodedMessage.getHeader();
        assertEquals(ReplicationProtocolUtils.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(ReplicationProtocolUtils.VERSION, header.getVersion());
        assertEquals(ReplicationProto.MessageType.CONFIRMATION, header.getMessageType());
        assertEquals(sequence, header.getSequence());
        
        ReplicationProto.ConfirmationMessage confirmationMessage = decodedMessage.getConfirmation();
        assertEquals(sequence, confirmationMessage.getSequence());
        assertEquals(nodeId, confirmationMessage.getNodeId());
        assertEquals(success, confirmationMessage.getSuccess());
        
        // 验证消息完整性
        assertTrue(ReplicationProtocolUtils.validateMessage(decodedMessage));
        
        encodedMessage.release();
    }
    
    @Test
    void testEncodeDecodeRegisterMessage() {
        String nodeId = "slave-001";
        String host = "192.168.1.100";
        int port = 8080;
        int replicationPort = 9090;
        ReplicationProto.NodeType nodeType = ReplicationProto.NodeType.SLAVE;
        
        // 编码注册消息
        ByteBuf encodedMessage = ReplicationProtocolUtils.encodeRegisterMessage(nodeId, host, port, replicationPort, nodeType);
        assertNotNull(encodedMessage);
        assertTrue(encodedMessage.readableBytes() > 0);
        
        // 解码消息
        ReplicationProto.ReplicationMessage decodedMessage = ReplicationProtocolUtils.decodeMessage(encodedMessage);
        assertNotNull(decodedMessage);
        
        // 验证消息内容
        ReplicationProto.ProtocolHeader header = decodedMessage.getHeader();
        assertEquals(ReplicationProtocolUtils.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(ReplicationProtocolUtils.VERSION, header.getVersion());
        assertEquals(ReplicationProto.MessageType.REGISTER, header.getMessageType());
        
        ReplicationProto.RegisterMessage registerMessage = decodedMessage.getRegister();
        assertEquals(nodeType, registerMessage.getNodeType());
        assertEquals(nodeId, registerMessage.getNodeId());
        assertEquals(host, registerMessage.getHost());
        assertEquals(port, registerMessage.getPort());
        assertEquals(replicationPort, registerMessage.getReplicationPort());
        
        // 验证消息完整性
        assertTrue(ReplicationProtocolUtils.validateMessage(decodedMessage));
        
        encodedMessage.release();
    }
    
    @Test
    void testEncodeDecodeHeartbeatMessage() {
        String nodeId = "slave-001";
        int sequence = 5;
        
        // 编码心跳消息
        ByteBuf encodedMessage = ReplicationProtocolUtils.encodeHeartbeatMessage(nodeId, sequence);
        assertNotNull(encodedMessage);
        assertTrue(encodedMessage.readableBytes() > 0);
        
        // 解码消息
        ReplicationProto.ReplicationMessage decodedMessage = ReplicationProtocolUtils.decodeMessage(encodedMessage);
        assertNotNull(decodedMessage);
        
        // 验证消息内容
        ReplicationProto.ProtocolHeader header = decodedMessage.getHeader();
        assertEquals(ReplicationProtocolUtils.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(ReplicationProtocolUtils.VERSION, header.getVersion());
        assertEquals(ReplicationProto.MessageType.HEARTBEAT, header.getMessageType());
        assertEquals(sequence, header.getSequence());
        
        ReplicationProto.HeartbeatMessage heartbeatMessage = decodedMessage.getHeartbeat();
        assertEquals(nodeId, heartbeatMessage.getNodeId());
        assertEquals(sequence, heartbeatMessage.getSequence());
        
        // 验证消息完整性
        assertTrue(ReplicationProtocolUtils.validateMessage(decodedMessage));
        
        encodedMessage.release();
    }
    
    @Test
    void testValidateMessage() {
        // 创建有效的消息
        NexusWrapper wrapper = new NexusWrapper(io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator.DEFAULT, 512);
        wrapper.setEventType(NexusWrapper.EventType.BUSINESS);
        ByteBuf encodedMessage = ReplicationProtocolUtils.encodeBusinessMessage(wrapper, 100L);
        ReplicationProto.ReplicationMessage message = ReplicationProtocolUtils.decodeMessage(encodedMessage);
        
        // 验证有效消息
        assertTrue(ReplicationProtocolUtils.validateMessage(message));
        
        encodedMessage.release();
    }
}
