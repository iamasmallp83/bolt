package com.cmex.bolt.core;

import com.lmax.disruptor.EventFactory;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.PooledByteBufAllocator;
import lombok.Getter;

public class NexusWrapper {

    @Getter
    private final ByteBuf buffer;

    public NexusWrapper(PooledByteBufAllocator allocator, int bufferSize) {
        // 预分配ByteBuf
        this.buffer = allocator.directBuffer(bufferSize);
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