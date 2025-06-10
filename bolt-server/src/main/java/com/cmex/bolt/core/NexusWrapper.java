package com.cmex.bolt.core;

import com.cmex.bolt.Nexus;
import com.lmax.disruptor.EventFactory;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;

public class NexusWrapper {

    private Nexus.NexusEvent type;

    @Getter
    private final ByteBuf buffer;

    public NexusWrapper(PooledByteBufAllocator allocator, int bufferSize) {
        // 预分配ByteBuf
        this.buffer = allocator.directBuffer(bufferSize);
    }

    /**
     * 重置ByteBuf状态，准备下次使用
     */
    public void reset() {
        if (buffer != null) {
            buffer.clear(); // 重置读写索引
        }
    }

    /**
     * 释放资源（在关闭时调用）
     */
    public void release() {
        if (buffer != null && buffer.refCnt() > 0) {
            buffer.release();
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