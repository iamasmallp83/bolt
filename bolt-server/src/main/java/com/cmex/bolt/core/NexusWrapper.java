package com.cmex.bolt.core;

import com.lmax.disruptor.EventFactory;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;
import lombok.Setter;

public class NexusWrapper {

    @Getter
    private final ByteBuf buffer;

    @Getter
    @Setter
    private long id;

    @Getter
    @Setter
    private int partition;

    @Getter
    @Setter
    private EventType eventType = EventType.BUSINESS;
    
    // 手动添加setter方法以避免Lombok问题
    public void setId(long id) {
        this.id = id;
    }
    
    public void setPartition(int partition) {
        this.partition = partition;
    }
    
    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }
    
    public ByteBuf getBuffer() {
        return buffer;
    }
    
    public long getId() {
        return id;
    }
    
    public int getPartition() {
        return partition;
    }
    
    public EventType getEventType() {
        return eventType;
    }

    /**
     * 获取合并的partition和eventType字段
     * eventType占据高3位，partition占据低7位
     *
     * @return 合并后的int值
     */
    public int getCombinedPartitionAndEventType() {
        return (eventType.getValue() << 7) | (partition & 0x7F);
    }

    /**
     * 设置合并的partition
     *
     * @param combined 合并后的int值
     */
    public void setPartitionByCombined(int combined) {
        this.partition = combined & 0x7F;
    }

    /**
     * 事件类型枚举
     */
    public enum EventType {
        BUSINESS(0),        // 业务产生的事件
        JOURNAL(1),         // 日志回放的事件
        SNAPSHOT(2),       // 快照事件
        INTERNAL(3);       // 内部产生的事件（如撮合产生的Clear事件）

        private final int value;

        EventType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static EventType fromValue(int value) {
            for (EventType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown EventType value: " + value);
        }
    }

    public NexusWrapper(PooledByteBufAllocator allocator, int bufferSize) {
        // 预分配ByteBuf
        this.buffer = allocator.directBuffer(bufferSize);
    }

    /**
     * 检查是否为业务事件（需要复制、日志记录、屏障处理）
     *
     * @return true 如果是业务事件，false 如果是回放或内部事件
     */
    public boolean isBusinessEvent() {
        return eventType == EventType.BUSINESS;
    }

    /**
     * 检查是否为回放事件（日志回放或从节点回放）
     *
     * @return true 如果是回放事件，false 如果不是
     */
    public boolean isJournalEvent() {
        return eventType == EventType.JOURNAL;
    }

    /**
     * 检查是否为快照事件
     *
     * @return true 如果是内部事件，false 如果不是
     */
    public boolean isSnapshotEvent() {
        return eventType == EventType.SNAPSHOT;
    }

    /**
     * 检查是否为内部产生的事件（如撮合产生的Clear事件）
     *
     * @return true 如果是内部事件，false 如果不是
     */
    public boolean isInternalEvent() {
        return eventType == EventType.INTERNAL;
    }
    
    /**
     * 检查是否为replication事件（从主节点复制的业务事件）
     *
     * @return true 如果是replication事件，false 如果不是
     */
    public boolean isReplicationEvent() {
        return eventType == EventType.BUSINESS; // replication事件也是业务事件
    }
    
    /**
     * 获取buffer的副本
     *
     * @return buffer的副本
     */
    public byte[] cloneBuffer() {
        byte[] copy = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), copy);
        return copy;
    }

    /**
     * NexusWrapper的副本类，包含所有必要的元数据
     */
    public static class NexusWrapperCopy {
        private final long id;
        private final int partition;
        private final EventType eventType;
        private final byte[] bufferData;

        public NexusWrapperCopy(long id, int partition, EventType eventType, byte[] bufferData) {
            this.id = id;
            this.partition = partition;
            this.eventType = eventType;
            this.bufferData = bufferData;
        }

        public long getId() {
            return id;
        }

        public int getPartition() {
            return partition;
        }

        public EventType getEventType() {
            return eventType;
        }

        public byte[] getBufferData() {
            return bufferData;
        }

        /**
         * 获取合并的partition和eventType字段
         */
        public int getCombinedPartitionAndEventType() {
            return (eventType.getValue() << 7) | (partition & 0x7F);
        }

        /**
         * 检查是否为业务事件
         */
        public boolean isBusinessEvent() {
            return eventType == EventType.BUSINESS;
        }

        /**
         * 检查是否为回放事件
         */
        public boolean isJournalEvent() {
            return eventType == EventType.JOURNAL;
        }

        /**
         * 检查是否为快照事件
         */
        public boolean isSnapshotEvent() {
            return eventType == EventType.SNAPSHOT;
        }

        /**
         * 检查是否为内部事件
         */
        public boolean isInternalEvent() {
            return eventType == EventType.INTERNAL;
        }

        @Override
        public String toString() {
            return String.format("NexusWrapperCopy{id=%d, partition=%d, eventType=%s, bufferSize=%d}",
                    id, partition, eventType, bufferData.length);
        }
    }

    /**
     * ByteBuf消息事件工厂
     */
    public static class Factory implements EventFactory<NexusWrapper> {
        private final PooledByteBufAllocator allocator;
        private final int bufferSize;

        public Factory(int bufferSize) {
            // 使用Netty的默认池化分配器
            this.allocator = PooledByteBufAllocator.DEFAULT;
            this.bufferSize = bufferSize;
        }

        @Override
        public NexusWrapper newInstance() {
            return new NexusWrapper(allocator, bufferSize);
        }
    }

}