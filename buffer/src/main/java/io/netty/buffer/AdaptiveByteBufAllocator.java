/*
 * Copyright 2024 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * An auto-tuning pooling {@link ByteBufAllocator}, that follows an anti-generational hypothesis.
 * <p>
 * <strong>Note:</strong> this allocator is <strong>experimental</strong>. It is recommended to roll out usage slowly,
 * and to carefully monitor application performance in the process.
 * <p>
 * See the {@link AdaptivePoolingAllocator} class documentation for implementation details.
 */
public final class AdaptiveByteBufAllocator extends AbstractByteBufAllocator
        implements ByteBufAllocatorMetricProvider, ByteBufAllocatorMetric {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(AdaptiveByteBufAllocator.class);
    // 是否对于NioEventLoop线程使用缓存的magazines 默认false
    private static final boolean DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS;

    static {
        DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS = SystemPropertyUtil.getBoolean(
                "io.netty.allocator.useCachedMagazinesForNonEventLoopThreads", false);
        logger.debug("-Dio.netty.allocator.useCachedMagazinesForNonEventLoopThreads: {}",
                     DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    private final AdaptivePoolingAllocator direct;
    private final AdaptivePoolingAllocator heap;

    public AdaptiveByteBufAllocator() {
        this(!PlatformDependent.isExplicitNoPreferDirect());
    }

    public AdaptiveByteBufAllocator(boolean preferDirect) {
        this(preferDirect, DEFAULT_USE_CACHED_MAGAZINES_FOR_NON_EVENT_LOOP_THREADS);
    }

    public AdaptiveByteBufAllocator(boolean preferDirect, boolean useCacheForNonEventLoopThreads) {
        super(preferDirect);
        direct = new AdaptivePoolingAllocator(new DirectChunkAllocator(this), useCacheForNonEventLoopThreads);
        heap = new AdaptivePoolingAllocator(new HeapChunkAllocator(this), useCacheForNonEventLoopThreads);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(heap.allocate(initialCapacity, maxCapacity));
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        return toLeakAwareBuffer(direct.allocate(initialCapacity, maxCapacity));
    }

    @Override
    public boolean isDirectBufferPooled() {
        return true;
    }

    @Override
    public long usedHeapMemory() {
        return heap.usedMemory();
    }

    @Override
    public long usedDirectMemory() {
        return direct.usedMemory();
    }

    @Override
    public ByteBufAllocatorMetric metric() {
        return this;
    }

    // 这个类不放到AdaptivePoolingAllocator类面，而是放到AdaptiveByteBufAllocator这个类里面，具体原因是什么？
    // 如果这个HeapChunkAllocator类只在当前外部类中被实例化和使用，那么将它们放在这里是符合逻辑的。这表明"这是属于这个类的私有实现细节",
    // 而不是一个可以被全局共享的通用组件。
    //  定义为外部类内部且为private,意味着外部世界，包括AdaptivePoolingAllocator这个类本身，完全不知道者两个具体实现的存在。他们只
    // 暴露ChunkAllocator这个接口行为。
    // 还可以防止其他代码直接依赖这两个具体的实现类，强制他们只依赖接口。
    private static final class HeapChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private HeapChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return PlatformDependent.hasUnsafe() ?
                    new UnpooledUnsafeHeapByteBuf(allocator, initialCapacity, maxCapacity) :
                    new UnpooledHeapByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }

    private static final class DirectChunkAllocator implements AdaptivePoolingAllocator.ChunkAllocator {
        private final ByteBufAllocator allocator;

        private DirectChunkAllocator(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public AbstractByteBuf allocate(int initialCapacity, int maxCapacity) {
            return UnsafeByteBufUtil.newDirectByteBuf(allocator, initialCapacity, maxCapacity);
        }
    }
}
