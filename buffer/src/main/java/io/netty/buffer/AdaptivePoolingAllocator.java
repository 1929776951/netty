/*
 * Copyright 2022 The Netty Project
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

import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.NettyRuntime;
import io.netty.util.Recycler;
import io.netty.util.Recycler.EnhancedHandle;
import io.netty.util.concurrent.ConcurrentSkipListIntObjMultimap;
import io.netty.util.concurrent.ConcurrentSkipListIntObjMultimap.IntEntry;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.concurrent.MpscIntQueue;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.RefCnt;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.ThreadExecutorMap;
import io.netty.util.internal.UnstableApi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.StampedLock;
import java.util.function.IntConsumer;

/**
 * An auto-tuning pooling allocator, that follows an anti-generational hypothesis.
 * <p>
 * The allocator is organized into a list of Magazines, and each magazine has a chunk-buffer that they allocate buffers
 * from.
 * <p>
 * The magazines hold the mutexes that ensure the thread-safety of the allocator, and each thread picks a magazine
 * based on the id of the thread. This spreads the contention of multi-threaded access across the magazines.
 * If contention is detected above a certain threshold, the number of magazines are increased in response to the
 * contention.
 * <p>
 * The magazines maintain histograms of the sizes of the allocations they do. The histograms are used to compute the
 * preferred chunk size. The preferred chunk size is one that is big enough to service 10 allocations of the
 * 99-percentile size. This way, the chunk size is adapted to the allocation patterns.
 * <p>
 * Computing the preferred chunk size is a somewhat expensive operation. Therefore, the frequency with which this is
 * done, is also adapted to the allocation pattern. If a newly computed preferred chunk is the same as the previous
 * preferred chunk size, then the frequency is reduced. Otherwise, the frequency is increased.
 * <p>
 * This allows the allocator to quickly respond to changes in the application workload,
 * without suffering undue overhead from maintaining its statistics.
 * <p>
 * Since magazines are "relatively thread-local", the allocator has a central queue that allow excess chunks from any
 * magazine, to be shared with other magazines.
 * The {@link #createSharedChunkQueue()} method can be overridden to customize this queue.
 */
@UnstableApi
final class AdaptivePoolingAllocator {
    // 最小内存阈值，小于这个就定义为小内存环境 512MB
    private static final int LOW_MEM_THRESHOLD = 512 * 1024 * 1024;
    private static final boolean IS_LOW_MEM = Runtime.getRuntime().maxMemory() <= LOW_MEM_THRESHOLD;

    /**
     * Whether the IS_LOW_MEM setting should disable thread-local magazines.
     * This can have fairly high performance overhead.
     */
    private static final boolean DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM = SystemPropertyUtil.getBoolean(
            "io.netty.allocator.disableThreadLocalMagazinesOnLowMemory", true);

    /**
     * The 128 KiB minimum chunk size is chosen to encourage the system allocator to delegate to mmap for chunk
     * allocations. For instance, glibc will do this.
     * This pushes any fragmentation from chunk size deviations off physical memory, onto virtual memory,
     * which is a much, much larger space. Chunks are also allocated in whole multiples of the minimum
     * chunk size, which itself is a whole multiple of popular page sizes like 4 KiB, 16 KiB, and 64 KiB.
     */
    // 这是分配器向操作系统申请内存的最小单位
    static final int MIN_CHUNK_SIZE = 128 * 1024;
    // 扩容尝试次数
    private static final int EXPANSION_ATTEMPTS = 3;
    // 初始仓库数
    private static final int INITIAL_MAGAZINES = 1;
    // 退役容量
    // 如果系统瞬间出现高负载,分配了大量的内存chunk,随后负载骤降,这些chunk如果一直保留在内存池中，会白白占用操作系统内存
    private static final int RETIRE_CAPACITY = 256;
    // 为了减少多线程环境下的锁竞争,采用了分片的机制，简单来说，Netty不会让所有线程都去抢同一块大内存池，而是创建了多个小的内存池
    // 这里是控制数量
    private static final int MAX_STRIPES = IS_LOW_MEM ? 1 : NettyRuntime.availableProcessors() * 2;
    // 每个 Chunk 期望容纳大约 8 个缓冲区 为什么是 8？ 这是一个经验值。
    // 如果每个 Chunk 里塞太多 Buffer，管理起来太复杂；塞太少，内存碎片率又高。
    private static final int BUFS_PER_CHUNK = 8; // For large buffers, aim to have about this many buffers per chunk.

    /**
     * The maximum size of a pooled chunk, in bytes. Allocations bigger than this will never be pooled.
     * <p>
     * This number is 8 MiB, and is derived from the limitations of internal histograms.
     */
    // 单个内存块（Chunk）能增长到的最大值。2MB 8MB
    private static final int MAX_CHUNK_SIZE = IS_LOW_MEM ?
            2 * 1024 * 1024 : // 2 MiB for systems with small heaps.
            8 * 1024 * 1024; // 8 MiB.
    // 申请MAX_POOLED_BUF_SIZE以下 的内存，走自适应池化逻辑（复用）
    private static final int MAX_POOLED_BUF_SIZE = MAX_CHUNK_SIZE / BUFS_PER_CHUNK;

    /**
     * The capacity if the chunk reuse queues, that allow chunks to be shared across magazines in a group.
     * The default size is twice {@link NettyRuntime#availableProcessors()},
     * same as the maximum number of magazines per magazine group.
     */
    // 跨线程共享（Chunk 复用）队列大小
    // 这是一个全局共享或组共享的队列。当某个线程（Magazine）持有的内存块（Chunk）空闲了，
    // 且该线程自己不需要时，它不会直接释放归还给操作系统，而是尝试把这个 Chunk 扔进这个“公共池子”里。其他缺内存的线程可以来这里“捡漏”。
    private static final int CHUNK_REUSE_QUEUE = Math.max(2, SystemPropertyUtil.getInt(
            "io.netty.allocator.chunkReuseQueueCapacity", NettyRuntime.availableProcessors() * 2));

    /**
     * The capacity if the magazine local buffer queue. This queue just pools the outer ByteBuf instance and not
     * the actual memory and so helps to reduce GC pressure.
     */
    // 线程本地的“对象缓存” 这个队列存的不是真正的内存（Memory），而是 ByteBuf 对象实例（Object Wrapper）。
// GC 的代价：当堆内存里充满了这种短命的 ByteBuf 对象时，JVM 会频繁触发 Young GC。
// 如果对象分配速度太快，甚至可能触发 Full GC，导致应用“卡顿”。
    //简单来说：存对象外壳的意义在于减少 Java 堆上的对象分配压力（GC），而不存内存是为了避免内存浪费（内存碎片）。
    //我们分两步来拆解你的疑问：
    //存“空壳对象”有什么意义？
    // 场景 A：如果不复用对象（完全 new）
    //new PooledByteBuf() -> 在堆上分配对象空间（有 GC 成本）。
    //调用 allocateMemory(...) -> 向操作系统/内存池申请真正的内存块（有同步/锁成本）。
    //场景 B：复用对象（Netty 的做法）
    //从 MAGAZINE_BUFFER_QUEUE 拿出一个旧的 PooledByteBuf。 -> 零 GC 成本。
    //调用 memory(...) -> 向操作系统/内存池申请真正的内存块。
    // 省去了 new 对象的过程，也省去了之后 GC 回收这个对象的过程。这对降低延迟（Latency）非常关键。
    private static final int MAGAZINE_BUFFER_QUEUE_CAPACITY = SystemPropertyUtil.getInt(
            "io.netty.allocator.magazineBufferQueueCapacity", 1024);

    /**
     * The size classes are chosen based on the following observation:
     * <p>
     * Most allocations, particularly ones above 256 bytes, aim to be a power-of-2. However, many use cases, such
     * as framing protocols, are themselves operating or moving power-of-2 sized payloads, to which they add a
     * small amount of overhead, such as headers or checksums.
     * This means we seem to get a lot of mileage out of having both power-of-2 sizes, and power-of-2-plus-a-bit.
     * <p>
     * On the conflicting requirements of both having as few chunks as possible, and having as little wasted
     * memory within each chunk as possible, this seems to strike a surprisingly good balance for the use cases
     * tested so far.
     */
    // 很多场景下，我们的业务数据（Payload）本身是 2 的幂大小（比如 512 字节的数据块）。
    // 但是，网络协议通常会在这些数据外面包一层“壳”（TCP 头、IP 头、应用层 Header、校验和等）。
    // 如果只有 2 的幂：512 不够装，必须分配 1024 的规格。
    //浪费：1024 - 612 = 412 字节 被浪费了！浪费率接近 40%。
    // Power-of-2 + 一点余地
    //Netty 引入了 640 这个规格。
    //场景：512 字节数据 + 128 字节头部 = 640 字节。
    //结果：刚好装进 640 的规格里，而不需要升级到 1024。
    //收益：极大地减少了内部碎片，提升了内存利用率。
    private static final int[] SIZE_CLASSES = {
            32,
            64,
            128,
            256,
            512,
            640, // 512 + 128
            1024,
            1152, // 1024 + 128
            2048,
            2304, // 2048 + 256
            4096,
            4352, // 4096 + 256
            8192,
            8704, // 8192 + 512
            16384,
            16896, // 16384 + 512
    };

    private static final int SIZE_CLASSES_COUNT = SIZE_CLASSES.length;
    private static final byte[] SIZE_INDEXES = new byte[SIZE_CLASSES[SIZE_CLASSES_COUNT - 1] / 32 + 1];

    static {
        if (MAGAZINE_BUFFER_QUEUE_CAPACITY < 2) {
            throw new IllegalArgumentException("MAGAZINE_BUFFER_QUEUE_CAPACITY: " + MAGAZINE_BUFFER_QUEUE_CAPACITY
                    + " (expected: >= " + 2 + ')');
        }
        int lastIndex = 0;
        for (int i = 0; i < SIZE_CLASSES_COUNT; i++) {
            int sizeClass = SIZE_CLASSES[i];
            //noinspection ConstantValue
            assert (sizeClass & 31) == 0 : "Size class must be a multiple of 32";
            int sizeIndex = sizeIndexOf(sizeClass);
            Arrays.fill(SIZE_INDEXES, lastIndex + 1, sizeIndex + 1, (byte) i);
            lastIndex = sizeIndex;
        }
    }
    // 它负责直接向操作系统（JVM 堆外内存或堆内内存）申请大块的原始内存（Chunk，默认 16MB）。
    private final ChunkAllocator chunkAllocator;
    // 它负责记录和追踪所有已经申请出来的 Chunk。
    private final ChunkRegistry chunkRegistry;
    // 下面两个是全局共享的 中央仓库
    // 尺寸类别类型的
    private final MagazineGroup[] sizeClassedMagazineGroups;
    // 大缓存区的
    private final MagazineGroup largeBufferMagazineGroup;
    // 这个是当前线程本地缓存的。
    private final FastThreadLocal<MagazineGroup[]> threadLocalGroup;

    AdaptivePoolingAllocator(ChunkAllocator chunkAllocator, boolean useCacheForNonEventLoopThreads) {
        this.chunkAllocator = ObjectUtil.checkNotNull(chunkAllocator, "chunkAllocator");
        chunkRegistry = new ChunkRegistry();
        sizeClassedMagazineGroups = createMagazineGroupSizeClasses(this, false);
        largeBufferMagazineGroup = new MagazineGroup(
                this, chunkAllocator, new BuddyChunkManagementStrategy(), false);

        boolean disableThreadLocalGroups = IS_LOW_MEM && DISABLE_THREAD_LOCAL_MAGAZINES_ON_LOW_MEM;
        threadLocalGroup = disableThreadLocalGroups ? null : new FastThreadLocal<MagazineGroup[]>() {
            @Override
            protected MagazineGroup[] initialValue() {
                if (useCacheForNonEventLoopThreads || ThreadExecutorMap.currentExecutor() != null) {
                    return createMagazineGroupSizeClasses(AdaptivePoolingAllocator.this, true);
                }
                return null;
            }

            @Override
            protected void onRemoval(final MagazineGroup[] groups) throws Exception {
                if (groups != null) {
                    for (MagazineGroup group : groups) {
                        group.free();
                    }
                }
            }
        };
    }

    private static MagazineGroup[] createMagazineGroupSizeClasses(
            AdaptivePoolingAllocator allocator, boolean isThreadLocal) {
        MagazineGroup[] groups = new MagazineGroup[SIZE_CLASSES.length];
        // 每个规格大小一个MagazineGroup
        for (int i = 0; i < SIZE_CLASSES.length; i++) {
            int segmentSize = SIZE_CLASSES[i];
            groups[i] = new MagazineGroup(allocator, allocator.chunkAllocator,
                    new SizeClassChunkManagementStrategy(segmentSize), isThreadLocal);
        }
        return groups;
    }

    /**
     * Create a thread-safe multi-producer, multi-consumer queue to hold chunks that spill over from the
     * internal Magazines.
     * <p>
     * Each Magazine can only hold two chunks at any one time: the chunk it currently allocates from,
     * and the next-in-line chunk which will be used for allocation once the current one has been used up.
     * This queue will be used by magazines to share any excess chunks they allocate, so that they don't need to
     * allocate new chunks when their current and next-in-line chunks have both been used up.
     * <p>
     * The simplest implementation of this method is to return a new {@link ConcurrentLinkedQueue}.
     * However, the {@code CLQ} is unbounded, and this means there's no limit to how many chunks can be cached in this
     * queue.
     * <p>
     * Each chunk in this queue can be up to {@link #MAX_CHUNK_SIZE} in size, so it is recommended to use a bounded
     * queue to limit the maximum memory usage.
     * <p>
     * The default implementation will create a bounded queue with a capacity of {@link #CHUNK_REUSE_QUEUE}.
     *
     * @return A new multi-producer, multi-consumer queue.
     */
    private static Queue<SizeClassedChunk> createSharedChunkQueue() {
        return PlatformDependent.newFixedMpmcQueue(CHUNK_REUSE_QUEUE);
    }

    ByteBuf allocate(int size, int maxCapacity) {
        return allocate(size, maxCapacity, Thread.currentThread(), null);
    }

    private AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
        AdaptiveByteBuf allocated = null;
        if (size <= MAX_POOLED_BUF_SIZE) {
            final int index = sizeClassIndexOf(size);
            MagazineGroup[] magazineGroups;
            // 先从本地线程里面获取
            if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals() ||
                    IS_LOW_MEM ||
                    (magazineGroups = threadLocalGroup.get()) == null) {
                magazineGroups =  sizeClassedMagazineGroups;
            }
            // 如果申请的size大小在规格内有，则从对应的magazineGroups中获取对应的magazineGroup
            if (index < magazineGroups.length) {
                allocated = magazineGroups[index].allocate(size, maxCapacity, currentThread, buf);
            } else if (!IS_LOW_MEM) {// 如果index不再设定的规格内,且不是小内存,才走大内存分配
                allocated = largeBufferMagazineGroup.allocate(size, maxCapacity, currentThread, buf);
            }
        }
        // 如果上面不再设定的规格内，也不走大内存分配，走这里的备选方案分配
        // fallback是后备方案
        if (allocated == null) {
            allocated = allocateFallback(size, maxCapacity, currentThread, buf);
        }
        return allocated;
    }

    private static int sizeIndexOf(final int size) {
        // this is aligning the size to the next multiple of 32 and dividing by 32 to get the size index.
        return size + 31 >> 5;
    }

    static int sizeClassIndexOf(int size) {
        int sizeIndex = sizeIndexOf(size);
        if (sizeIndex < SIZE_INDEXES.length) {
            return SIZE_INDEXES[sizeIndex];
        }
        return SIZE_CLASSES_COUNT;
    }

    static int[] getSizeClasses() {
        return SIZE_CLASSES.clone();
    }

    private AdaptiveByteBuf allocateFallback(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
        // If we don't already have a buffer, obtain one from the most conveniently available magazine.
        Magazine magazine;
        if (buf != null) {
            Chunk chunk = buf.chunk;
            if (chunk == null || chunk == Magazine.MAGAZINE_FREED || (magazine = chunk.currentMagazine()) == null) {
                magazine = getFallbackMagazine(currentThread);
            }
        } else {
            magazine = getFallbackMagazine(currentThread);
            buf = magazine.newBuffer();
        }
        // Create a one-off chunk for this allocation.
        AbstractByteBuf innerChunk = chunkAllocator.allocate(size, maxCapacity);
        Chunk chunk = new Chunk(innerChunk, magazine, false);
        chunkRegistry.add(chunk);
        try {
            boolean success = chunk.readInitInto(buf, size, size, maxCapacity);
            assert success: "Failed to initialize ByteBuf with dedicated chunk";
        } finally {
            // As the chunk is an one-off we need to always call release explicitly as readInitInto(...)
            // will take care of retain once when successful. Once The AdaptiveByteBuf is released it will
            // completely release the Chunk and so the contained innerChunk.
            chunk.release();
        }
        return buf;
    }

    private Magazine getFallbackMagazine(Thread currentThread) {
        Magazine[] mags = largeBufferMagazineGroup.magazines;
        return mags[(int) currentThread.getId() & mags.length - 1];
    }

    /**
     * Allocate into the given buffer. Used by {@link AdaptiveByteBuf#capacity(int)}.
     */
    void reallocate(int size, int maxCapacity, AdaptiveByteBuf into) {
        AdaptiveByteBuf result = allocate(size, maxCapacity, Thread.currentThread(), into);
        assert result == into: "Re-allocation created separate buffer instance";
    }

    long usedMemory() {
        return chunkRegistry.totalCapacity();
    }

    // Ensure that we release all previous pooled resources when this object is finalized. This is needed as otherwise
    // we might end up with leaks. While these leaks are usually harmless in reality it would still at least be
    // very confusing for users.
    @SuppressWarnings({"FinalizeDeclaration", "deprecation"})
    @Override
    protected void finalize() throws Throwable {
        try {
            free();
        } finally {
            super.finalize();
        }
    }

    private void free() {
        largeBufferMagazineGroup.free();
    }

    private static final class MagazineGroup {
        private final AdaptivePoolingAllocator allocator;
        private final ChunkAllocator chunkAllocator;
        private final ChunkManagementStrategy chunkManagementStrategy;
        private final ChunkCache chunkCache;
        private final StampedLock magazineExpandLock;
        private final Magazine threadLocalMagazine;
        private Thread ownerThread;
        private volatile Magazine[] magazines;
        private volatile boolean freed;

        MagazineGroup(AdaptivePoolingAllocator allocator,
                      ChunkAllocator chunkAllocator,
                      ChunkManagementStrategy chunkManagementStrategy,
                      boolean isThreadLocal) {
            this.allocator = allocator;
            this.chunkAllocator = chunkAllocator;
            this.chunkManagementStrategy = chunkManagementStrategy;
            chunkCache = chunkManagementStrategy.createChunkCache(isThreadLocal);
            if (isThreadLocal) {
                ownerThread = Thread.currentThread();
                magazineExpandLock = null;
                threadLocalMagazine = new Magazine(this, false, chunkManagementStrategy.createController(this));
            } else {
                ownerThread = null;
                magazineExpandLock = new StampedLock();
                threadLocalMagazine = null;
                Magazine[] mags = new Magazine[INITIAL_MAGAZINES];
                for (int i = 0; i < mags.length; i++) {
                    mags[i] = new Magazine(this, true, chunkManagementStrategy.createController(this));
                }
                magazines = mags;
            }
        }

        // 如果是扩容操作reallocate，则传入旧的bug，如果是新分配，传入null
        // 当长度是2的幂次方时,用threadId & mask，代替threadId % mask ，性能更高
        // 当l = 2 ^ k 时  x mod l = x & (l -1)
        //  一个数x 除以 2 ^1 余数就是最后一位
        //                // 一个数x 除以 2 ^2 余数就是最后二位
        //                //一个数x 除以 2 ^3 余数就是最后三位
        //                //一个数x 除以 2 ^4 余数就是最后四位
        //                //一个数x 除以 2 ^5 余数就是最后五位
        //                // 要求余数 也即是求这个数的底幂次方位,
        //                // 2幂次方的数有一个规律就是只有1个1，后面全是0，几次幂就是几个0
        //                // 二2的幂次方-1 是几次幂就几个1  那么这两个数据按位与就是底几位(幂次方)
        public AdaptiveByteBuf allocate(int size, int maxCapacity, Thread currentThread, AdaptiveByteBuf buf) {
            boolean reallocate = buf != null;

            // Path for thread-local allocation.
            Magazine tlMag = threadLocalMagazine;
            if (tlMag != null) {
                if (buf == null) {
                    buf = tlMag.newBuffer();
                }
                boolean allocated = tlMag.tryAllocate(size, maxCapacity, buf, reallocate);
                assert allocated : "Allocation of threadLocalMagazine must always succeed";
                return buf;
            }

            // Path for concurrent allocation.
            long threadId = currentThread.getId();
            Magazine[] mags;
            // 记录扩容的次数
            int expansions = 0;
            do {
                mags = magazines;
                int mask = mags.length - 1;
                int index = (int) (threadId & mask);
                // 为什么是2倍的长度,比如你到银行办业务，一共8个窗口，然后你应该去3好窗口，你到了3好窗口，发现有人在办业务，你不需要等待，直接
                // 去4号、5号、6号、7号 ,0号 、1号、2号、3号、4号、5号、6号、7号、0号、1号、2号
                // 循环两倍的长度，是为了增加尝试的次数，给予更多的机会去"碰运气"找到一个空闲。

                // 同一个线程每次拿到的Magazine可能不一样 Netty的设计就是 只要能抢到内存，管它是哪个Magazine，谁空闲我就用谁
                for (int i = 0, m = mags.length << 1; i < m; i++) {
                    Magazine mag = mags[index + i & mask];
                    // 下面是先分配，在尝试 减小锁的粒度。
                    if (buf == null) {
                        // 这里只拿到了 AdaptiveByteBuf的空对象，没有真正分配底层内存
                        buf = mag.newBuffer();
                    }
                    if (mag.tryAllocate(size, maxCapacity, buf, reallocate)) {
                        // Was able to allocate.
                        return buf;
                    }
                }
                expansions++;
            } while (expansions <= EXPANSION_ATTEMPTS && tryExpandMagazines(mags.length));

            // The magazines failed us; contention too high and we don't want to spend more effort expanding the array.
            if (!reallocate && buf != null) {
                buf.release(); // Release the previously claimed buffer before we return.
            }
            return null;
        }

        private boolean tryExpandMagazines(int currentLength) {
            if (currentLength >= MAX_STRIPES) {
                return true;
            }
            final Magazine[] mags;
            long writeLock = magazineExpandLock.tryWriteLock();
            if (writeLock != 0) {
                try {
                    mags = magazines;
                    if (mags.length >= MAX_STRIPES || mags.length > currentLength || freed) {
                        return true;
                    }
                    Magazine[] expanded = new Magazine[mags.length * 2];
                    for (int i = 0, l = expanded.length; i < l; i++) {
                        expanded[i] = new Magazine(this, true, chunkManagementStrategy.createController(this));
                    }
                    magazines = expanded;
                } finally {
                    magazineExpandLock.unlockWrite(writeLock);
                }
                for (Magazine magazine : mags) {
                    magazine.free();
                }
            }
            return true;
        }

        Chunk pollChunk(int size) {
            return chunkCache.pollChunk(size);
        }

        boolean offerChunk(Chunk chunk) {
            if (freed) {
                return false;
            }

            if (chunk.hasUnprocessedFreelistEntries()) {
                chunk.processFreelistEntries();
            }
            boolean isAdded = chunkCache.offerChunk(chunk);

            if (freed && isAdded) {
                // Help to free the reuse queue.
                freeChunkReuseQueue(ownerThread);
            }
            return isAdded;
        }

        private void free() {
            freed = true;
            Thread ownerThread = this.ownerThread;
            if (threadLocalMagazine != null) {
                this.ownerThread = null;
                threadLocalMagazine.free();
            } else {
                long stamp = magazineExpandLock.writeLock();
                try {
                    Magazine[] mags = magazines;
                    for (Magazine magazine : mags) {
                        magazine.free();
                    }
                } finally {
                    magazineExpandLock.unlockWrite(stamp);
                }
            }
            freeChunkReuseQueue(ownerThread);
        }

        private void freeChunkReuseQueue(Thread ownerThread) {
            Chunk chunk;
            while ((chunk = chunkCache.pollChunk(0)) != null) {
                if (ownerThread != null && chunk instanceof SizeClassedChunk) {
                    SizeClassedChunk threadLocalChunk = (SizeClassedChunk) chunk;
                    assert ownerThread == threadLocalChunk.ownerThread;
                    // no release segment can ever happen from the owner Thread since it's not running anymore
                    // This is required to let the ownerThread to be GC'ed despite there are AdaptiveByteBuf
                    // that reference some thread local chunk
                    threadLocalChunk.ownerThread = null;
                }
                chunk.markToDeallocate();
            }
        }
    }

    private interface ChunkCache {
        Chunk pollChunk(int size);
        boolean offerChunk(Chunk chunk);
    }

    private static final class ConcurrentQueueChunkCache implements ChunkCache {
        private final Queue<SizeClassedChunk> queue;

        private ConcurrentQueueChunkCache() {
            queue = createSharedChunkQueue();
        }

        @Override
        public SizeClassedChunk pollChunk(int size) {
            // we really don't care about size here since the sized class chunk q
            // just care about segments of fixed size!
            Queue<SizeClassedChunk> queue = this.queue;
            for (int i = 0; i < CHUNK_REUSE_QUEUE; i++) {
                SizeClassedChunk chunk = queue.poll();
                if (chunk == null) {
                    return null;
                }
                if (chunk.hasRemainingCapacity()) {
                    return chunk;
                }
                queue.offer(chunk);
            }
            return null;
        }

        @Override
        public boolean offerChunk(Chunk chunk) {
            return queue.offer((SizeClassedChunk) chunk);
        }
    }

    private static final class ConcurrentSkipListChunkCache implements ChunkCache {
        private final ConcurrentSkipListIntObjMultimap<Chunk> chunks;

        private ConcurrentSkipListChunkCache() {
            chunks = new ConcurrentSkipListIntObjMultimap<>(-1);
        }

        @Override
        public Chunk pollChunk(int size) {
            if (chunks.isEmpty()) {
                return null;
            }
            IntEntry<Chunk> entry = chunks.pollCeilingEntry(size);
            if (entry != null) {
                Chunk chunk = entry.getValue();
                if (chunk.hasUnprocessedFreelistEntries()) {
                    chunk.processFreelistEntries();
                }
                return chunk;
            }

            Chunk bestChunk = null;
            int bestRemainingCapacity = 0;
            Iterator<IntEntry<Chunk>> itr = chunks.iterator();
            while (itr.hasNext()) {
                entry = itr.next();
                final Chunk chunk;
                if (entry != null && (chunk = entry.getValue()).hasUnprocessedFreelistEntries()) {
                    if (!chunks.remove(entry.getKey(), entry.getValue())) {
                        continue;
                    }
                    chunk.processFreelistEntries();
                    int remainingCapacity = chunk.remainingCapacity();
                    if (remainingCapacity >= size &&
                            (bestChunk == null || remainingCapacity > bestRemainingCapacity)) {
                        if (bestChunk != null) {
                            chunks.put(bestRemainingCapacity, bestChunk);
                        }
                        bestChunk = chunk;
                        bestRemainingCapacity = remainingCapacity;
                    } else {
                        chunks.put(remainingCapacity, chunk);
                    }
                }
            }

            return bestChunk;
        }

        @Override
        public boolean offerChunk(Chunk chunk) {
            chunks.put(chunk.remainingCapacity(), chunk);

            int size = chunks.size();
            while (size > CHUNK_REUSE_QUEUE) {
                // Deallocate the chunk with the fewest incoming references.
                int key = -1;
                Chunk toDeallocate = null;
                for (IntEntry<Chunk> entry : chunks) {
                    Chunk candidate = entry.getValue();
                    if (candidate != null) {
                        if (toDeallocate == null) {
                            toDeallocate = candidate;
                            key = entry.getKey();
                        } else {
                            int candidateRefCnt = RefCnt.refCnt(candidate.refCnt);
                            int toDeallocateRefCnt = RefCnt.refCnt(toDeallocate.refCnt);
                            if (candidateRefCnt < toDeallocateRefCnt ||
                                    candidateRefCnt == toDeallocateRefCnt &&
                                            candidate.capacity() < toDeallocate.capacity()) {
                                toDeallocate = candidate;
                                key = entry.getKey();
                            }
                        }
                    }
                }
                if (toDeallocate == null) {
                    break;
                }
                if (chunks.remove(key, toDeallocate)) {
                    toDeallocate.markToDeallocate();
                }
                size = chunks.size();
            }
            return true;
        }
    }

    private interface ChunkManagementStrategy {
        ChunkController createController(MagazineGroup group);

        ChunkCache createChunkCache(boolean isThreadLocal);
    }

    private interface ChunkController {
        /**
         * Compute the "fast max capacity" value for the buffer.
         */
        int computeBufferCapacity(int requestedSize, int maxCapacity, boolean isReallocation);

        /**
         * Allocate a new {@link Chunk} for the given {@link Magazine}.
         */
        Chunk newChunkAllocation(int promptingSize, Magazine magazine);
    }

    private static final class SizeClassChunkManagementStrategy implements ChunkManagementStrategy {
        // To amortize activation/deactivation of chunks, we should have a minimum number of segments per chunk.
        // We choose 32 because it seems neither too small nor too big.
        // For segments of 16 KiB, the chunks will be half a megabyte.
        private static final int MIN_SEGMENTS_PER_CHUNK = 32;
        private final int segmentSize;
        private final int chunkSize;

        private SizeClassChunkManagementStrategy(int segmentSize) {
            this.segmentSize = ObjectUtil.checkPositive(segmentSize, "segmentSize");
            chunkSize = Math.max(MIN_CHUNK_SIZE, segmentSize * MIN_SEGMENTS_PER_CHUNK);
        }

        @Override
        public ChunkController createController(MagazineGroup group) {
            return new SizeClassChunkController(group, segmentSize, chunkSize);
        }

        @Override
        public ChunkCache createChunkCache(boolean isThreadLocal) {
            return new ConcurrentQueueChunkCache();
        }
    }

    private static final class SizeClassChunkController implements ChunkController {

        private final ChunkAllocator chunkAllocator;
        private final int segmentSize;
        private final int chunkSize;
        private final ChunkRegistry chunkRegistry;

        private SizeClassChunkController(MagazineGroup group, int segmentSize, int chunkSize) {
            chunkAllocator = group.chunkAllocator;
            this.segmentSize = segmentSize;
            this.chunkSize = chunkSize;
            chunkRegistry = group.allocator.chunkRegistry;
        }

        private MpscIntQueue createEmptyFreeList() {
            return MpscIntQueue.create(chunkSize / segmentSize, SizeClassedChunk.FREE_LIST_EMPTY);
        }

        private MpscIntQueue createFreeList() {
            final int segmentsCount = chunkSize / segmentSize;
            final MpscIntQueue freeList = MpscIntQueue.create(segmentsCount, SizeClassedChunk.FREE_LIST_EMPTY);
            int segmentOffset = 0;
            for (int i = 0; i < segmentsCount; i++) {
                freeList.offer(segmentOffset);
                segmentOffset += segmentSize;
            }
            return freeList;
        }

        private IntStack createLocalFreeList() {
            final int segmentsCount = chunkSize / segmentSize;
            int segmentOffset = chunkSize;
            int[] offsets = new int[segmentsCount];
            for (int i = 0; i < segmentsCount; i++) {
                segmentOffset -= segmentSize;
                offsets[i] = segmentOffset;
            }
            return new IntStack(offsets);
        }

        @Override
        public int computeBufferCapacity(
                int requestedSize, int maxCapacity, boolean isReallocation) {
            return Math.min(segmentSize, maxCapacity);
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            AbstractByteBuf chunkBuffer = chunkAllocator.allocate(chunkSize, chunkSize);
            assert chunkBuffer.capacity() == chunkSize;
            SizeClassedChunk chunk = new SizeClassedChunk(chunkBuffer, magazine, this);
            chunkRegistry.add(chunk);
            return chunk;
        }
    }

    // 该类是ChunkManagementStrategy策略的一个实现，这个是管理大内存的，还有一个是尺寸规格管理的策略
    // 本类不是真正管理的，是创建真正管理的Chunk的controller和chunkCache两个组件的工厂类

    private static final class BuddyChunkManagementStrategy implements ChunkManagementStrategy {
        private final AtomicInteger maxChunkSize = new AtomicInteger();

        // 负责创建一个BuddyChunkController
        @Override
        public ChunkController createController(MagazineGroup group) {
            // 这个控制器是内存管理的"大脑",它内部会利用经典的Buddy System(伙伴算法)来管理内存的分配与回收
            return new BuddyChunkController(group, maxChunkSize);
        }

        @Override
        public ChunkCache createChunkCache(boolean isThreadLocal) {
            // 这个缓存是内存管理的"仓库",用于存放空闲的、可供复用的内存块 这个使用的是跳表，确保在多线程环境下，对空闲内存块的查找、
            // 插入和删除操作都能高效且线程安全完成
            return new ConcurrentSkipListChunkCache();
        }
    }

    private static final class BuddyChunkController implements ChunkController {
        private final ChunkAllocator chunkAllocator;
        private final ChunkRegistry chunkRegistry;
        private final AtomicInteger maxChunkSize;

        BuddyChunkController(MagazineGroup group, AtomicInteger maxChunkSize) {
            chunkAllocator = group.chunkAllocator;
            chunkRegistry = group.allocator.chunkRegistry;
            this.maxChunkSize = maxChunkSize;
        }

        @Override
        public int computeBufferCapacity(int requestedSize, int maxCapacity, boolean isReallocation) {
            return MathUtil.safeFindNextPositivePowerOfTwo(requestedSize);
        }

        @Override
        public Chunk newChunkAllocation(int promptingSize, Magazine magazine) {
            int maxChunkSize = this.maxChunkSize.get();
            int proposedChunkSize = MathUtil.safeFindNextPositivePowerOfTwo(BUFS_PER_CHUNK * promptingSize);
            int chunkSize = Math.min(MAX_CHUNK_SIZE, Math.max(maxChunkSize, proposedChunkSize));
            if (chunkSize > maxChunkSize) {
                // Update our stored max chunk size. It's fine that this is racy.
                this.maxChunkSize.set(chunkSize);
            }
            BuddyChunk chunk = new BuddyChunk(chunkAllocator.allocate(chunkSize, chunkSize), magazine);
            chunkRegistry.add(chunk);
            return chunk;
        }
    }

    // 如果把整个内存池比作一个“大型图书馆”，Magazine 就是每个“借书窗口”。
    // 每个线程（或每组线程）会绑定一个 Magazine。当线程需要分配内存时，它首先尝试从自己绑定的 Magazine 中快速分配，尽量避免去争夺全局的大锁。
    // 分配时：线程找到绑定的 Magazine -> 检查 current 是否有空间 -> 有则直接分配（极快） -> 无则看 nextInline ->
    // 还没有则通过 group 和 chunkController 申请新 Chunk。
    private static final class Magazine {
        private static final AtomicReferenceFieldUpdater<Magazine, Chunk> NEXT_IN_LINE;
        static {
            NEXT_IN_LINE = AtomicReferenceFieldUpdater.newUpdater(Magazine.class, Chunk.class, "nextInLine");
        }
        // 哨兵对象”（Sentinel Value）。当 nextInline 字段被设置为这个值时，
        // 意味着当前的 Magazine 已经没有更多的预分配内存了，或者该资源已经被释放。这是一种高效的空值检查机制，避免了频繁的 null 判断。
        private static final Chunk MAGAZINE_FREED = new Chunk();

        private static final class AdaptiveRecycler extends Recycler<AdaptiveByteBuf> {

            private AdaptiveRecycler(boolean unguarded) {
                // uses fast thread local
                super(unguarded);
            }

            private AdaptiveRecycler(int maxCapacity, boolean unguarded) {
                // doesn't use fast thread local, shared
                super(maxCapacity, unguarded);
            }

            @Override
            protected AdaptiveByteBuf newObject(final Handle<AdaptiveByteBuf> handle) {
                return new AdaptiveByteBuf((EnhancedHandle<AdaptiveByteBuf>) handle);
            }

            public static AdaptiveRecycler threadLocal() {
                return new AdaptiveRecycler(true);
            }

            public static AdaptiveRecycler sharedWith(int maxCapacity) {
                return new AdaptiveRecycler(maxCapacity, true);
            }
        }

        private static final AdaptiveRecycler EVENT_LOOP_LOCAL_BUFFER_POOL = AdaptiveRecycler.threadLocal();

        // 当前正在使用的内存块
        private Chunk current;
        @SuppressWarnings("unused") // updated via NEXT_IN_LINE
        // 下一个预取的内存块 这是一个预取机制。当 current 快用完时，Magazine 会提前从全局或组里申请下一个 Chunk 放在这里
        // 使用 volatile 保证多线程可见性（虽然 Magazine 主要是线程绑定，但在某些迁移或共享场景下可能被其他线程访问）
        private volatile Chunk nextInLine;
        // 指向拥有当前Magazine的组
        private final MagazineGroup group;
        // Chunk 的管理员 它负责管理 Magazine 内部的 Chunk 列表（比如将用完的 Chunk 放入空闲列表，
        // 或者从空闲列表获取 Chunk）。它封装了对 Chunk 生命周期的具体操作逻辑
        private final ChunkController chunkController;
        private final StampedLock allocationLock;
        private final AdaptiveRecycler recycler;

        Magazine(MagazineGroup group, boolean shareable, ChunkController chunkController) {
            this.group = group;
            this.chunkController = chunkController;

            if (shareable) {
                // We only need the StampedLock if this Magazine will be shared across threads.
                allocationLock = new StampedLock();
                recycler = AdaptiveRecycler.sharedWith(MAGAZINE_BUFFER_QUEUE_CAPACITY);
            } else {
                allocationLock = null;
                recycler = null;
            }
        }

        public boolean tryAllocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
            if (allocationLock == null) {
                // 没有锁 代表当前线程独享Magazine，不需要加锁，直接分配内存
                // 只有 allocationLock被跨线程共享时才会被初始化
                // This magazine is not shared across threads, just allocate directly.
                return allocate(size, maxCapacity, buf, reallocate);
            }

            // Try to retrieve the lock and if successful allocate.
            long writeLock = allocationLock.tryWriteLock();
            // 尝试获取一个写锁，如果成功，返回一个非0的锁句柄
            if (writeLock != 0) {
                try {
                    return allocate(size, maxCapacity, buf, reallocate);
                } finally {
                    allocationLock.unlockWrite(writeLock);
                }
            }
            // 如果拿不到锁，说明当前这个共享的Magazine正忙，具体看方法内部的逻辑。
            return allocateWithoutLock(size, maxCapacity, buf);
        }

        private boolean allocateWithoutLock(int size, int maxCapacity, AdaptiveByteBuf buf) {
            Chunk curr = NEXT_IN_LINE.getAndSet(this, null);
            if (curr == MAGAZINE_FREED) {
                // Allocation raced with a stripe-resize that freed this magazine.
                restoreMagazineFreed();
                return false;
            }
            if (curr == null) {
                curr = group.pollChunk(size);
                if (curr == null) {
                    return false;
                }
                curr.attachToMagazine(this);
            }
            boolean allocated = false;
            int remainingCapacity = curr.remainingCapacity();
            int startingCapacity = chunkController.computeBufferCapacity(
                    size, maxCapacity, true /* never update stats as we don't hold the magazine lock */);
            if (remainingCapacity >= size &&
                    curr.readInitInto(buf, size, Math.min(remainingCapacity, startingCapacity), maxCapacity)) {
                allocated = true;
                remainingCapacity = curr.remainingCapacity();
            }
            try {
                if (remainingCapacity >= RETIRE_CAPACITY) {
                    transferToNextInLineOrRelease(curr);
                    curr = null;
                }
            } finally {
                if (curr != null) {
                    curr.releaseFromMagazine();
                }
            }
            return allocated;
        }

        private boolean allocate(int size, int maxCapacity, AdaptiveByteBuf buf, boolean reallocate) {
            int startingCapacity = chunkController.computeBufferCapacity(size, maxCapacity, reallocate);
            Chunk curr = current;
            if (curr != null) {
                boolean success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                int remainingCapacity = curr.remainingCapacity();
                if (!success && remainingCapacity > 0) {
                    current = null;
                    transferToNextInLineOrRelease(curr);
                } else if (remainingCapacity == 0) {
                    current = null;
                    curr.releaseFromMagazine();
                }
                if (success) {
                    return true;
                }
            }

            assert current == null;
            // The fast-path for allocations did not work.
            //
            // Try to fetch the next "Magazine local" Chunk first, if this fails because we don't have a
            // next-in-line chunk available, we will poll our centralQueue.
            // If this fails as well we will just allocate a new Chunk.
            //
            // In any case we will store the Chunk as the current so it will be used again for the next allocation and
            // thus be "reserved" by this Magazine for exclusive usage.
            curr = NEXT_IN_LINE.getAndSet(this, null);
            if (curr != null) {
                if (curr == MAGAZINE_FREED) {
                    // Allocation raced with a stripe-resize that freed this magazine.
                    restoreMagazineFreed();
                    return false;
                }

                int remainingCapacity = curr.remainingCapacity();
                if (remainingCapacity > startingCapacity &&
                        curr.readInitInto(buf, size, startingCapacity, maxCapacity)) {
                    // We have a Chunk that has some space left.
                    current = curr;
                    return true;
                }

                try {
                    if (remainingCapacity >= size) {
                        // At this point we know that this will be the last time curr will be used, so directly set it
                        // to null and release it once we are done.
                        return curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                    }
                } finally {
                    // Release in a finally block so even if readInitInto(...) would throw we would still correctly
                    // release the current chunk before null it out.
                    curr.releaseFromMagazine();
                }
            }

            // Now try to poll from the central queue first
            curr = group.pollChunk(size);
            if (curr == null) {
                curr = chunkController.newChunkAllocation(size, this);
            } else {
                curr.attachToMagazine(this);

                int remainingCapacity = curr.remainingCapacity();
                if (remainingCapacity == 0 || remainingCapacity < size) {
                    // Check if we either retain the chunk in the nextInLine cache or releasing it.
                    if (remainingCapacity < RETIRE_CAPACITY) {
                        curr.releaseFromMagazine();
                    } else {
                        // See if it makes sense to transfer the Chunk to the nextInLine cache for later usage.
                        // This method will release curr if this is not the case
                        transferToNextInLineOrRelease(curr);
                    }
                    curr = chunkController.newChunkAllocation(size, this);
                }
            }

            current = curr;
            boolean success;
            try {
                int remainingCapacity = curr.remainingCapacity();
                assert remainingCapacity >= size;
                if (remainingCapacity > startingCapacity) {
                    success = curr.readInitInto(buf, size, startingCapacity, maxCapacity);
                    curr = null;
                } else {
                    success = curr.readInitInto(buf, size, remainingCapacity, maxCapacity);
                }
            } finally {
                if (curr != null) {
                    // Release in a finally block so even if readInitInto(...) would throw we would still correctly
                    // release the current chunk before null it out.
                    curr.releaseFromMagazine();
                    current = null;
                }
            }
            return success;
        }

        private void restoreMagazineFreed() {
            Chunk next = NEXT_IN_LINE.getAndSet(this, MAGAZINE_FREED);
            if (next != null && next != MAGAZINE_FREED) {
                // A chunk snuck in through a race. Release it after restoring MAGAZINE_FREED state.
                next.releaseFromMagazine();
            }
        }

        private void transferToNextInLineOrRelease(Chunk chunk) {
            if (NEXT_IN_LINE.compareAndSet(this, null, chunk)) {
                return;
            }

            Chunk nextChunk = NEXT_IN_LINE.get(this);
            if (nextChunk != null && nextChunk != MAGAZINE_FREED
                    && chunk.remainingCapacity() > nextChunk.remainingCapacity()) {
                if (NEXT_IN_LINE.compareAndSet(this, nextChunk, chunk)) {
                    nextChunk.releaseFromMagazine();
                    return;
                }
            }
            // Next-in-line is occupied. We don't try to add it to the central queue yet as it might still be used
            // by some buffers and so is attached to a Magazine.
            // Once a Chunk is completely released by Chunk.release() it will try to move itself to the queue
            // as last resort.
            chunk.releaseFromMagazine();
        }

        void free() {
            // Release the current Chunk and the next that was stored for later usage.
            restoreMagazineFreed();
            long stamp = allocationLock != null ? allocationLock.writeLock() : 0;
            try {
                if (current != null) {
                    current.releaseFromMagazine();
                    current = null;
                }
            } finally {
                if (allocationLock != null) {
                    allocationLock.unlockWrite(stamp);
                }
            }
        }

        public AdaptiveByteBuf newBuffer() {
            AdaptiveRecycler recycler = this.recycler;
            AdaptiveByteBuf buf = recycler == null? EVENT_LOOP_LOCAL_BUFFER_POOL.get() : recycler.get();
            // 这里为什么只清理引用计数 和  markedReaderIndex  markedWriterIndex
            buf.resetRefCnt();
            buf.discardMarks();
            return buf;
        }

        boolean offerToQueue(Chunk chunk) {
            return group.offerChunk(chunk);
        }
    }

    // Netty通过AdaptivePoolingAllocator分配的所有内存块的总量.它是一个全局的内存计数器
    private static final class ChunkRegistry {
        // 用来存储已经分配出去的内存字节总数
        // LongAdder转为解决AtomicLong在高并发场景下的性能瓶颈而设计。
        private final LongAdder totalCapacity = new LongAdder();

        public long totalCapacity() {
            return totalCapacity.sum();
        }

        public void add(Chunk chunk) {
            totalCapacity.add(chunk.capacity());
        }

        public void remove(Chunk chunk) {
            totalCapacity.add(-chunk.capacity());
        }
    }
   // 如果把 Magazine 比作“钱包”，那么 Chunk 就是钱包里的“大额钞票” 它代表了从操作系统申请来的一大块原始内存（通常是 16MB）。
    private static class Chunk implements ChunkInfo {
        // 真正的内存容器
       // 如果是堆外内存它实际上是一个 DirectByteBuf，底层持有一个 sun.misc.Unsafe 指向的直接内存地址
       // 如果是堆内内存（Heap Memory），它实际上是一个 byte[]
        // 所有的读写操作最终都会委托给这个 delegate
        protected final AbstractByteBuf delegate;
        // 它记录了这个 Chunk 属于哪个 Magazine（内存仓库）。
        protected Magazine magazine;
        // 指向最顶层的内存分配器  Chunk 需要扩容、销毁或者发生特殊情况时，它会通过这个 allocator 向上层请求资源或汇报状态。
       // 最顶层”指的是整个内存分配体系的总入口和总管理者。Netty 的内存池是一个典型的树状层级结构，从上到下依次是
       // 第 1 层（最顶层）：AdaptivePoolingAllocator 它管理着下一层的 MagazineGroup 数组。
       // 第 2 层（中间层）：MagazineGroup 它管理着一组 Magazine。 它的存在主要是为了适应多核 CPU，
        // 减少伪共享（False Sharing）和锁竞争。它负责在多个 Magazine 之间做负载均衡。
       // 第 3 层（核心层）：Magazine 一线主管 / 线程本地仓库 它是线程（Thread）直接打交道的对象。 它持有 current Chunk（当前正在用的内存块）。
        //它负责快速的内存分配（从 current 切分）和回收。
       // 第 4 层（物理层）：Chunk 这是实际存储数据的内存块
       // 假设你手里拿着一个 Chunk,正在往里写数据。突然，数据写满了，但用户调用 buffer.capacity(newSize) 想要把缓冲区变得更大。
       // Chunk 自己不知道去哪里申请新的、更大的内存块。它只是一块死板的内存。
       // 它必须找到最顶层的 AdaptivePoolingAllocator，因为只有顶层分配器拥有“向操作系统申请新内存”的最高权限和全局视野。
        private final AdaptivePoolingAllocator allocator;
        // Always populate the refCnt field, so HotSpot doesn't emit `null` checks.
        // This is safe to do even on native-image.
       // Netty 使用引用计数来管理内存的生命周期 每当有一个地方在使用这个 Chunk，计数加 1；使用完毕释放时，计数减 1。
       // 初始化这个字段是为了让 HotSpot JVM 避免做昂贵的空指针检查（Null Check），这是一种微优化技巧
        private final RefCnt refCnt = new RefCnt();
       // 记录这个 Chunk 的大小（字节数） 容量上限
        private final int capacity;
        // 标记这个 Chunk 是否应该被放回池子里复用
        private final boolean pooled;
        // 已分配统计 记录当前这个 Chunk 里已经分出去了多少字节
        protected int allocatedBytes;

        Chunk() {
            // 注释写得很清楚，这个构造函数仅用于创建 MAGAZINE_FREED 哨兵对象
            // Constructor only used by the MAGAZINE_FREED sentinel.
            delegate = null;
            magazine = null;
            allocator = null;
            capacity = 0;
            pooled = false;
        }

        Chunk(AbstractByteBuf delegate, Magazine magazine, boolean pooled) {
            this.delegate = delegate;
            this.pooled = pooled;
            capacity = delegate.capacity();
            attachToMagazine(magazine);

            // We need the top-level allocator so ByteBuf.capacity(int) can call reallocate()
            // 向上追溯，找到最顶层的 AdaptivePoolingAllocator
            allocator = magazine.group.allocator;

            if (PlatformDependent.isJfrEnabled() && AllocateChunkEvent.isEventEnabled()) {
                AllocateChunkEvent event = new AllocateChunkEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.pooled = pooled;
                    event.threadLocal = magazine.allocationLock == null;
                    event.commit();
                }
            }
        }

        Magazine currentMagazine()  {
            return magazine;
        }
// 切断Chunk 与其所属 Magazine 之间的关联 detach 解绑
       //1、当一个 Chunk 被完全释放（即不再被任何业务逻辑使用），
// 并且决定不再将其放回 Magazine 的空闲列表中时（例如内存池收缩，或者 Chunk 被销毁），需要调用此方法来清理状态
       // 2、如果这个 Chunk 需要从一个 Magazine 转移到另一个 Magazine
// （虽然 Netty 的设计倾向于线程本地分配，但在某些负载不均的情况下可能会发生窃取或转移
       // 3、Magazine 通常与特定的线程或线程组强关联 如果一个 Chunk 被长时间持有（例如被用户代码持有引用），
// 而线程结束了或者 Magazine 被销毁了，此时如果不手动 detach，Chunk 就会持有一个指向已失效 Magazine 的引用，
// 导致 Magazine 无法被垃圾回收（内存泄漏）
        void detachFromMagazine() {
            if (magazine != null) {
                magazine = null;
            }
        }

        void attachToMagazine(Magazine magazine) {
            assert this.magazine == null;
            this.magazine = magazine;
        }

        /**
         * Called when a magazine is done using this chunk, probably because it was emptied.
         */
        void releaseFromMagazine() {
            // Chunks can be reused before they become empty.
            // We can therefor put them in the shared queue as soon as the magazine is done with this chunk.
            // 这个 Chunk 现在空闲了，或者至少当前持有它的 Magazine 不想管它了，我可以讲这个chunk放入共享队列
            Magazine mag = magazine;
            // 解绑当前chunk和magazine
            detachFromMagazine();
            // 将chunk放入共享队列
            if (!mag.offerToQueue(this)) {
                // 如果放入失败 (可能是因为内存池已经够大了，不需要这么多空闲 Chunk) 记这个 Chunk 等待被销毁
                markToDeallocate();
            }
        }

        /**
         * Called when a ByteBuf is done using its allocation in this chunk.
         */
        void releaseSegment(int ignoredSegmentId, int size) {
            release();
        }

        void markToDeallocate() {
            release();
        }

        private void retain() {
                RefCnt.retain(refCnt);
        }

        protected boolean release() {
            boolean deallocate = RefCnt.release(refCnt);
            if (deallocate) {
                deallocate();
            }
            return deallocate;
        }

        protected void deallocate() {
            onRelease();
            allocator.chunkRegistry.remove(this);
            delegate.release();
        }

        private void onRelease() {
            if (PlatformDependent.isJfrEnabled() && FreeChunkEvent.isEventEnabled()) {
                FreeChunkEvent event = new FreeChunkEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.pooled = pooled;
                    event.commit();
                }
            }
        }

        // 初始化传入的 buf，使其指向当前 Chunk 中的一块新内存区域。
        public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            int startIndex = allocatedBytes;
            allocatedBytes = startIndex + startingCapacity;
            Chunk chunk = this;
            chunk.retain();
            try {
                buf.init(delegate, chunk, 0, 0, startIndex, size, startingCapacity, maxCapacity);
                chunk = null;
            } finally {
                if (chunk != null) {
                    // If chunk is not null we know that buf.init(...) failed and so we need to manually release
                    // the chunk again as we retained it before calling buf.init(...). Beside this we also need to
                    // restore the old allocatedBytes value.
                    allocatedBytes = startIndex;
                    chunk.release();
                }
            }
            return true;
        }

        public int remainingCapacity() {
            return capacity - allocatedBytes;
        }

        public boolean hasUnprocessedFreelistEntries() {
            return false;
        }

        public void processFreelistEntries() {
        }

        @Override
        public int capacity() {
            return capacity;
        }

        @Override
        public boolean isDirect() {
            return delegate.isDirect();
        }

        @Override
        public long memoryAddress() {
            return delegate._memoryAddress();
        }
    }

    // 它专门用于存储 int 类型的原始数据，避免了 Java 集合（如 Stack<Integer>）带来的自动装箱/拆箱开销
    private static final class IntStack {

        private final int[] stack;
        private int top;

        IntStack(int[] initialValues) {
            stack = initialValues;
            top = initialValues.length - 1;
        }

        public boolean isEmpty() {
            return top == -1;
        }

        public int pop() {
            final int last = stack[top];
            top--;
            return last;
        }

        public void push(int value) {
            stack[top + 1] = value;
            top++;
        }

        public int size() {
            return top + 1;
        }
    }

    /**
     * Removes per-allocation retain()/release() atomic ops from the hot path by replacing ref counting
     * with a segment-count state machine. Atomics are only needed on the cold deallocation path
     * ({@link #markToDeallocate()}), which is rare for long-lived chunks that cycle segments many times.
     * The tradeoff is a {@link MpscIntQueue#size()} call (volatile reads, no RMW) per remaining segment
     * return after mark — acceptable since it avoids atomic RMWs entirely.
     * <p>
     * State transitions:
     * <ul>
     *   <li>{@link #AVAILABLE} (-1): chunk is in use, no deallocation tracking needed</li>
     *   <li>0..N: local free list size at the time {@link #markToDeallocate()} was called;
     *       used to track when all segments have been returned</li>
     *   <li>{@link #DEALLOCATED} (Integer.MIN_VALUE): all segments returned, chunk deallocated</li>
     * </ul>
     * <p>
     * Ordering: external {@link #releaseSegment} pushes to the MPSC queue (which has an implicit
     * StoreLoad barrier via its {@code offer()}), then reads {@code state} — this guarantees
     * visibility of any preceding {@link #markToDeallocate()} write.
     */
    private static final class SizeClassedChunk extends Chunk {
        // 标记空闲列表已空。
        private static final int FREE_LIST_EMPTY = -1;
        // 表示 Chunk 已初始化，可以被分配。
        private static final int AVAILABLE = -1;
        // Integer.MIN_VALUE so that `DEALLOCATED + externalFreeList.size()` can never equal `segments`,
        // making late-arriving releaseSegment calls on external threads arithmetically harmless.
        // 使用 Integer.MIN_VALUE 是为了防止算术溢出导致的误判（如注释所述），标记 Chunk 已被彻底释放。
        private static final int DEALLOCATED = Integer.MIN_VALUE;
        // 可以在不加锁的情况下，原子地更新 Chunk 的状态
        private static final AtomicIntegerFieldUpdater<SizeClassedChunk> STATE =
            AtomicIntegerFieldUpdater.newUpdater(SizeClassedChunk.class, "state");
        private volatile int state;
        // 每个小内存卡的大小
        private final int segments;
        // 有多少个小内存快
        private final int segmentSize;
        // 外部空闲队列。如果其他线程来释放这块内存，或者没有绑定线程，则使用 MpscIntQueue
        private final MpscIntQueue externalFreeList;
        // 本地空闲栈 存储空闲索引
        private final IntStack localFreeList;
        // 当前chunk属于这个线程
        private Thread ownerThread;

        SizeClassedChunk(AbstractByteBuf delegate, Magazine magazine,
                         SizeClassChunkController controller) {
            super(delegate, magazine, true);
            segmentSize = controller.segmentSize;
            segments = controller.chunkSize / segmentSize;
            STATE.lazySet(this, AVAILABLE);
            ownerThread = magazine.group.ownerThread;
            if (ownerThread == null) {
                externalFreeList = controller.createFreeList();
                localFreeList = null;
            } else {
                externalFreeList = controller.createEmptyFreeList();
                localFreeList = controller.createLocalFreeList();
            }
        }

        @Override
        public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            assert state == AVAILABLE;
            final int startIndex = nextAvailableSegmentOffset();
            if (startIndex == FREE_LIST_EMPTY) {
                return false;
            }
            allocatedBytes += segmentSize;
            try {
                buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
            } catch (Throwable t) {
                allocatedBytes -= segmentSize;
                releaseSegmentOffsetIntoFreeList(startIndex);
                throw t;
            }
            return true;
        }

        private int nextAvailableSegmentOffset() {
            final int startIndex;
            IntStack localFreeList = this.localFreeList;
            if (localFreeList != null) {
                assert Thread.currentThread() == ownerThread;
                if (localFreeList.isEmpty()) {
                    startIndex = externalFreeList.poll();
                } else {
                    startIndex = localFreeList.pop();
                }
            } else {
                startIndex = externalFreeList.poll();
            }
            return startIndex;
        }

        // this can be used by the ConcurrentQueueChunkCache to find the first buffer to use:
        // it doesn't update the remaining capacity and it's not consider a single segmentSize
        // case as not suitable to be reused
        public boolean hasRemainingCapacity() {
            int remaining = super.remainingCapacity();
            if (remaining > 0) {
                return true;
            }
            if (localFreeList != null) {
                return !localFreeList.isEmpty();
            }
            return !externalFreeList.isEmpty();
        }

        @Override
        public int remainingCapacity() {
            int remaining = super.remainingCapacity();
            return remaining > segmentSize ? remaining : updateRemainingCapacity(remaining);
        }

        private int updateRemainingCapacity(int snapshotted) {
            int freeSegments = externalFreeList.size();
            IntStack localFreeList = this.localFreeList;
            if (localFreeList != null) {
                freeSegments += localFreeList.size();
            }
            int updated = freeSegments * segmentSize;
            if (updated != snapshotted) {
                allocatedBytes = capacity() - updated;
            }
            return updated;
        }

        private void releaseSegmentOffsetIntoFreeList(int startIndex) {
            IntStack localFreeList = this.localFreeList;
            if (localFreeList != null && Thread.currentThread() == ownerThread) {
                localFreeList.push(startIndex);
            } else {
                boolean segmentReturned = externalFreeList.offer(startIndex);
                assert segmentReturned : "Unable to return segment " + startIndex + " to free list";
            }
        }

        @Override
        void releaseSegment(int startIndex, int size) {
            IntStack localFreeList = this.localFreeList;
            if (localFreeList != null && Thread.currentThread() == ownerThread) {
                localFreeList.push(startIndex);
                int state = this.state;
                if (state != AVAILABLE) {
                    updateStateOnLocalReleaseSegment(state, localFreeList);
                }
            } else {
                boolean segmentReturned = externalFreeList.offer(startIndex);
                assert segmentReturned;
                // implicit StoreLoad barrier from MPSC offer()
                int state = this.state;
                if (state != AVAILABLE) {
                    deallocateIfNeeded(state);
                }
            }
        }

        private void updateStateOnLocalReleaseSegment(int previousLocalSize, IntStack localFreeList) {
            int newLocalSize = localFreeList.size();
            boolean alwaysTrue = STATE.compareAndSet(this, previousLocalSize, newLocalSize);
            assert alwaysTrue : "this shouldn't happen unless double release in the local free list";
            deallocateIfNeeded(newLocalSize);
        }

        private void deallocateIfNeeded(int localSize) {
            // Check if all segments have been returned.
            int totalFreeSegments = localSize + externalFreeList.size();
            if (totalFreeSegments == segments && STATE.compareAndSet(this, localSize, DEALLOCATED)) {
                deallocate();
            }
        }

        @Override
        void markToDeallocate() {
            IntStack localFreeList = this.localFreeList;
            int localSize = localFreeList != null ? localFreeList.size() : 0;
            STATE.set(this, localSize);
            deallocateIfNeeded(localSize);
        }
    }

    // Buddy 伙伴 伙伴内存分配算法
    private static final class BuddyChunk extends Chunk implements IntConsumer {
        // 最小分配单元是 32KB
        private static final int MIN_BUDDY_SIZE = 32768;
        // 一个字节8位 1左移7位 最高位1 来表示某个内存块是否被使用
        private static final byte IS_CLAIMED = (byte) (1 << 7);
        // 一个字节8位 1左移6位 第二高位1 来表示某个内存块的子/孙节点是否被使用
        private static final byte HAS_CLAIMED_CHILDREN = 1 << 6;
        // 计算内存大小的对数掩码 SHIFT_MASK为11000000 用这个可以计算某个内存块的大小的对数
        private static final byte SHIFT_MASK = ~(IS_CLAIMED | HAS_CLAIMED_CHILDREN);
        // 想使用一个整数表示两个信息 一个是内存块的大小级别（Size Shift）,前16位最大是65535，而根本用不到2^65535大小，这是天文数字了，
        // 2^40次方都已经是1TB了。 在 Netty 中，一个标准的 Chunk 大小固定为 16 MB。24 远远小于 16 位能容纳的 65535。
        // 所以，从绝对大小的上限来看，高 16 位不仅够用，而且极其富余。
        // 另一个是代表这块内存的起始位置（即它是第几块）。 16MB 的偏移量用 16 位（最大 65535）是绝对不够的。
        // 16 MB = 16 * 1024 * 1024 = 16,777,216 字节。 显然，65,535 远远小于 1600多万，如果存字节偏移量，绝对会溢出
        // 所以这里存的不是“第几个字节”，而是“第几个 32KB 的小块”
        // 一个内存块四32KB，所以16MB/32KB=512, 一个 16MB 的 Chunk，最多只会被切分成 512 个 最小内存块
        // 而 16 位能存的最大数字是 65,535。 512 连 65,535 的零头都不到，所以用低 16 位来存这个偏移量，不仅够用，简直是绰绰有余
        // 00000000 00000000 11111111 11111111
        //  怎么把下面5和10从一个整数中拆包出来呢
        // 327780 & PACK_OFFSET_MASK 相当于把前 16 位强行清零，只剩后 16 位 得到100，
        // 327780 >>> PACK_SIZE_SHIFT 相当于把后 16 位直接砍掉，把高位拉下来 得到5
        private static final int PACK_OFFSET_MASK = 0xFFFF;
        // 如果你想把另一个数字拼接到 0xFFFF 的左边（高位），你需要把它向左移动 PACK_SIZE_SHIFT 位
        // int 打包后的结果 = (5 << PACK_SIZE_SHIFT) | 100; 这个值为327780， 在二进制里，它就是前半截存着 5，后半截存着 100）
        private static final int PACK_SIZE_SHIFT = Integer.SIZE - Integer.numberOfLeadingZeros(PACK_OFFSET_MASK);
        // 这是一个多生产者单消费者队列，专门用来存储 int 类型的整数。
        // 当有内存块被释放时 Netty会把代表内存信息的打包后的整数存储到这个队列
        // 因为 Netty 的高性能特性，可能会有多个线程同时释放内存（生产者多），但通常由一个线程负责处理分配逻辑（消费者单），
        // 这种队列在并发环境下比标准的 LinkedList 或 ArrayBlockingQueue 更快，因为它减少了锁的竞争
        private final MpscIntQueue freeList;
        // The bits of each buddy: [1: is claimed][1: has claimed children][30: MIN_BUDDY_SIZE shift to get size]
        // 这是伙伴算法的元数据核心。它并不存储实际数据，而是存储每个内存块的状态。
        private final byte[] buddies;
        // 记录 freeList 的容量上限 freeList的size大小
        private final int freeListCapacity;

        BuddyChunk(AbstractByteBuf delegate, Magazine magazine) {
            super(delegate, magazine, true);
            // 一般默认的delegate.capacity() 是16MB,MIN_BUDDY_SIZE是32KB freeListCapacity是512
            freeListCapacity = delegate.capacity() / MIN_BUDDY_SIZE;
            // Integer.numberOfTrailingZeros()这个方法的作用是计算一个整数的二进制表示中，末尾有多少个连续的 0。
            // 末尾有多少个0就代表是2的多少次方 这里这个值代表二叉树的层级或者是深度
            int maxShift = Integer.numberOfTrailingZeros(freeListCapacity);
            assert maxShift <= 30; // The top 2 bits are used for marking.
            // 传-1表示先别急着分配大数组，先用小的，不够了再自动长大
            freeList = MpscIntQueue.create(freeListCapacity, -1); // At most half of tree (all leaf nodes) can be freed.
            // 对于满二叉树 总节点数 = 2 × 叶子节点数 - 1
            // 叶子节点数是512个,总结点数需要1023 这里分配1024  索引 0 通常不用，或者仅仅作为填充
            buddies = new byte[freeListCapacity << 1];

            // Generate the buddies entries.
            int index = 1;
            int runLength = 1;
            int currentRun = 0;
            while (maxShift > 0) {
                buddies[index++] = (byte) maxShift;
                if (++currentRun == runLength) {
                    currentRun = 0;
                    runLength <<= 1;
                    maxShift--;
                }
            }
        }

        // AdaptiveByteBuf buf 需要被初始化的目标缓冲区对象
        // size 请求大小
        // startingCapacity 初始容量
        // maxCapacity 最大容量
        @Override
        public boolean readInitInto(AdaptiveByteBuf buf, int size, int startingCapacity, int maxCapacity) {
            if (!freeList.isEmpty()) {
                freeList.drain(freeListCapacity, this);
            }
            int startIndex = chooseFirstFreeBuddy(1, startingCapacity, 0);
            if (startIndex == -1) {
                return false;
            }
            Chunk chunk = this;
            chunk.retain();
            try {
                buf.init(delegate, this, 0, 0, startIndex, size, startingCapacity, maxCapacity);
                allocatedBytes += startingCapacity;
                chunk = null;
            } finally {
                if (chunk != null) {
                    unreserveMatchingBuddy(1, startingCapacity, startIndex, 0);
                    // If chunk is not null we know that buf.init(...) failed and so we need to manually release
                    // the chunk again as we retained it before calling buf.init(...).
                    chunk.release();
                }
            }
            return true;
        }

        @Override
        public void accept(int packed) {
            // Called by allocating thread when draining freeList.
            int size = unpackSize(packed);
            int offset = unpackOffset(packed);
            unreserveMatchingBuddy(1, size, offset, 0);
            allocatedBytes -= size;
        }

        private static int unpackSize(int packed) {
            return MIN_BUDDY_SIZE << (packed >> PACK_SIZE_SHIFT);
        }

        private static int unpackOffset(int packed) {
            return (packed & PACK_OFFSET_MASK) * MIN_BUDDY_SIZE;
        }

        @Override
        void releaseSegment(int startingIndex, int size) {
            int packedOffset = startingIndex / MIN_BUDDY_SIZE;
            int packedSize = Integer.numberOfTrailingZeros(size / MIN_BUDDY_SIZE) << PACK_SIZE_SHIFT;
            int packed = packedOffset | packedSize;
            freeList.offer(packed);
            release();
        }

        @Override
        public int remainingCapacity() {
            int capacityInFreeList = 0;
            if (!freeList.isEmpty()) {
                capacityInFreeList = freeList.weakPeekReduce(freeListCapacity, 0,
                        (sum, entry) -> sum + unpackSize(entry));
            }
            return super.remainingCapacity() + capacityInFreeList;
        }

        @Override
        public boolean hasUnprocessedFreelistEntries() {
            return !freeList.isEmpty();
        }

        @Override
        public void processFreelistEntries() {
            freeList.drain(freeListCapacity, this);
        }

        /**
         * Claim a suitable buddy and return its start offset into the delegate chunk, or return -1 if nothing claimed.
         */
        private int chooseFirstFreeBuddy(int index, int size, int currOffset) {
            byte[] buddies = this.buddies;
            while (index < buddies.length) {
                byte buddy = buddies[index];
                int currValue = MIN_BUDDY_SIZE << (buddy & SHIFT_MASK);
                if (currValue < size || (buddy & IS_CLAIMED) == IS_CLAIMED) {
                    return -1;
                }
                if (currValue == size && (buddy & HAS_CLAIMED_CHILDREN) == 0) {
                    buddies[index] |= IS_CLAIMED;
                    return currOffset;
                }
                int found = chooseFirstFreeBuddy(index << 1, size, currOffset);
                if (found != -1) {
                    buddies[index] |= HAS_CLAIMED_CHILDREN;
                    return found;
                }
                index = (index << 1) + 1;
                currOffset += currValue >> 1; // Bump offset to skip first half of this layer.
            }
            return -1;
        }

        /**
         * Un-reserve the matching buddy and return whether there are any other child or sibling reservations.
         */
        private boolean unreserveMatchingBuddy(int index, int size, int offset, int currOffset) {
            byte[] buddies = this.buddies;
            if (buddies.length <= index) {
                return false;
            }
            byte buddy = buddies[index];
            int currSize = MIN_BUDDY_SIZE << (buddy & SHIFT_MASK);

            if (currSize == size) {
                // We're at the right size level.
                if (currOffset == offset) {
                    buddies[index] &= SHIFT_MASK;
                    return false;
                }
                throw new IllegalStateException("The intended segment was not found at index " +
                        index + ", for size " + size + " and offset " + offset);
            }

            // We're at a parent size level. Use the target offset to guide our drill-down path.
            boolean claims;
            int siblingIndex;
            if (offset < currOffset + (currSize >> 1)) {
                // Must be down the left path.
                claims = unreserveMatchingBuddy(index << 1, size, offset, currOffset);
                siblingIndex = (index << 1) + 1;
            } else {
                // Must be down the rigth path.
                claims = unreserveMatchingBuddy((index << 1) + 1, size, offset, currOffset + (currSize >> 1));
                siblingIndex = index << 1;
            }
            if (!claims) {
                // No other claims down the path we took. Check if the sibling has claims.
                byte sibling = buddies[siblingIndex];
                if ((sibling & SHIFT_MASK) == sibling) {
                    // No claims in the sibling. We can clear this level as well.
                    buddies[index] &= SHIFT_MASK;
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            int capacity = delegate.capacity();
            int remaining = capacity - allocatedBytes;
            return "BuddyChunk[capacity: " + capacity +
                    ", remaining: " + remaining +
                    ", free list: " + freeList.size() + ']';
        }
    }

    static final class AdaptiveByteBuf extends AbstractReferenceCountedByteBuf {

        private final EnhancedHandle<AdaptiveByteBuf> handle;

        // this both act as adjustment and the start index for a free list segment allocation
        private int startIndex;
        // 真正的ByteBuf
        private AbstractByteBuf rootParent;
        Chunk chunk;
        private int length;
        private int maxFastCapacity;
        private ByteBuffer tmpNioBuf;
        private boolean hasArray;
        private boolean hasMemoryAddress;

        AdaptiveByteBuf(EnhancedHandle<AdaptiveByteBuf> recyclerHandle) {
            super(0);
            handle = ObjectUtil.checkNotNull(recyclerHandle, "recyclerHandle");
        }

        void init(AbstractByteBuf unwrapped, Chunk wrapped, int readerIndex, int writerIndex,
                  int startIndex, int size, int capacity, int maxCapacity) {
            this.startIndex = startIndex;
            chunk = wrapped;
            length = size;
            maxFastCapacity = capacity;
            maxCapacity(maxCapacity);
            setIndex0(readerIndex, writerIndex);
            hasArray = unwrapped.hasArray();
            hasMemoryAddress = unwrapped.hasMemoryAddress();
            rootParent = unwrapped;
            tmpNioBuf = null;

            if (PlatformDependent.isJfrEnabled() && AllocateBufferEvent.isEventEnabled()) {
                AllocateBufferEvent event = new AllocateBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.chunkPooled = wrapped.pooled;
                    Magazine m = wrapped.magazine;
                    event.chunkThreadLocal = m != null && m.allocationLock == null;
                    event.commit();
                }
            }
        }

        private AbstractByteBuf rootParent() {
            final AbstractByteBuf rootParent = this.rootParent;
            if (rootParent != null) {
                return rootParent;
            }
            throw new IllegalReferenceCountException();
        }

        @Override
        public int capacity() {
            return length;
        }

        @Override
        public int maxFastWritableBytes() {
            return Math.min(maxFastCapacity, maxCapacity()) - writerIndex;
        }

        @Override
        public ByteBuf capacity(int newCapacity) {
            checkNewCapacity(newCapacity);
            if (length <= newCapacity && newCapacity <= maxFastCapacity) {
                length = newCapacity;
                return this;
            }
            if (newCapacity < capacity()) {
                length = newCapacity;
                trimIndicesToCapacity(newCapacity);
                return this;
            }

            if (PlatformDependent.isJfrEnabled() && ReallocateBufferEvent.isEventEnabled()) {
                ReallocateBufferEvent event = new ReallocateBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.newCapacity = newCapacity;
                    event.commit();
                }
            }

            // Reallocation required.
            Chunk chunk = this.chunk;
            AdaptivePoolingAllocator allocator = chunk.allocator;
            int readerIndex = this.readerIndex;
            int writerIndex = this.writerIndex;
            int baseOldRootIndex = startIndex;
            int oldLength = length;
            int oldCapacity = maxFastCapacity;
            AbstractByteBuf oldRoot = rootParent();
            allocator.reallocate(newCapacity, maxCapacity(), this);
            oldRoot.getBytes(baseOldRootIndex, this, 0, oldLength);
            chunk.releaseSegment(baseOldRootIndex, oldCapacity);
            assert oldCapacity < maxFastCapacity && newCapacity <= maxFastCapacity:
                    "Capacity increase failed";
            this.readerIndex = readerIndex;
            this.writerIndex = writerIndex;
            return this;
        }

        @Override
        public ByteBufAllocator alloc() {
            return rootParent().alloc();
        }

        @SuppressWarnings("deprecation")
        @Override
        public ByteOrder order() {
            return rootParent().order();
        }

        @Override
        public ByteBuf unwrap() {
            return null;
        }

        @Override
        public boolean isDirect() {
            return rootParent().isDirect();
        }

        @Override
        public int arrayOffset() {
            return idx(rootParent().arrayOffset());
        }

        @Override
        public boolean hasMemoryAddress() {
            return hasMemoryAddress;
        }

        @Override
        public long memoryAddress() {
            ensureAccessible();
            return _memoryAddress();
        }

        @Override
        long _memoryAddress() {
            AbstractByteBuf root = rootParent;
            return root != null ? root._memoryAddress() + startIndex : 0L;
        }

        @Override
        boolean _isDirect() {
            AbstractByteBuf root = rootParent;
            return root != null && root.isDirect();
        }

        @Override
        public ByteBuffer nioBuffer(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffer(idx(index), length);
        }

        @Override
        public ByteBuffer internalNioBuffer(int index, int length) {
            checkIndex(index, length);
            return (ByteBuffer) internalNioBuffer().position(index).limit(index + length);
        }

        private ByteBuffer internalNioBuffer() {
            if (tmpNioBuf == null) {
                tmpNioBuf = rootParent().nioBuffer(startIndex, maxFastCapacity);
            }
            return (ByteBuffer) tmpNioBuf.clear();
        }

        @Override
        public ByteBuffer[] nioBuffers(int index, int length) {
            checkIndex(index, length);
            return rootParent().nioBuffers(idx(index), length);
        }

        @Override
        public boolean hasArray() {
            return hasArray;
        }

        @Override
        public byte[] array() {
            ensureAccessible();
            return rootParent().array();
        }

        @Override
        public ByteBuf copy(int index, int length) {
            checkIndex(index, length);
            return rootParent().copy(idx(index), length);
        }

        @Override
        public int nioBufferCount() {
            return rootParent().nioBufferCount();
        }

        @Override
        protected byte _getByte(int index) {
            return rootParent()._getByte(idx(index));
        }

        @Override
        protected short _getShort(int index) {
            return rootParent()._getShort(idx(index));
        }

        @Override
        protected short _getShortLE(int index) {
            return rootParent()._getShortLE(idx(index));
        }

        @Override
        protected int _getUnsignedMedium(int index) {
            return rootParent()._getUnsignedMedium(idx(index));
        }

        @Override
        protected int _getUnsignedMediumLE(int index) {
            return rootParent()._getUnsignedMediumLE(idx(index));
        }

        @Override
        protected int _getInt(int index) {
            return rootParent()._getInt(idx(index));
        }

        @Override
        protected int _getIntLE(int index) {
            return rootParent()._getIntLE(idx(index));
        }

        @Override
        protected long _getLong(int index) {
            return rootParent()._getLong(idx(index));
        }

        @Override
        protected long _getLongLE(int index) {
            return rootParent()._getLongLE(idx(index));
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
            checkIndex(index, length);
            rootParent().getBytes(idx(index), dst, dstIndex, length);
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, ByteBuffer dst) {
            checkIndex(index, dst.remaining());
            rootParent().getBytes(idx(index), dst);
            return this;
        }

        @Override
        protected void _setByte(int index, int value) {
            rootParent()._setByte(idx(index), value);
        }

        @Override
        protected void _setShort(int index, int value) {
            rootParent()._setShort(idx(index), value);
        }

        @Override
        protected void _setShortLE(int index, int value) {
            rootParent()._setShortLE(idx(index), value);
        }

        @Override
        protected void _setMedium(int index, int value) {
            rootParent()._setMedium(idx(index), value);
        }

        @Override
        protected void _setMediumLE(int index, int value) {
            rootParent()._setMediumLE(idx(index), value);
        }

        @Override
        protected void _setInt(int index, int value) {
            rootParent()._setInt(idx(index), value);
        }

        @Override
        protected void _setIntLE(int index, int value) {
            rootParent()._setIntLE(idx(index), value);
        }

        @Override
        protected void _setLong(int index, long value) {
            rootParent()._setLong(idx(index), value);
        }

        @Override
        protected void _setLongLE(int index, long value) {
            rootParent().setLongLE(idx(index), value);
        }

        @Override
        public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
            checkIndex(index, length);
            if (tmpNioBuf == null && PlatformDependent.javaVersion() >= 13) {
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), src, srcIndex, length);
            } else {
                ByteBuffer tmp = (ByteBuffer) internalNioBuffer().clear().position(index);
                tmp.put(src, srcIndex, length);
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
            checkIndex(index, length);
            if (src instanceof AdaptiveByteBuf && PlatformDependent.javaVersion() >= 16) {
                AdaptiveByteBuf srcBuf = (AdaptiveByteBuf) src;
                srcBuf.checkIndex(srcIndex, length);
                ByteBuffer dstBuffer = rootParent()._internalNioBuffer();
                ByteBuffer srcBuffer = srcBuf.rootParent()._internalNioBuffer();
                PlatformDependent.absolutePut(dstBuffer, idx(index), srcBuffer, srcBuf.idx(srcIndex), length);
            } else {
                ByteBuffer tmp = internalNioBuffer();
                tmp.position(index);
                tmp.put(src.nioBuffer(srcIndex, length));
            }
            return this;
        }

        @Override
        public ByteBuf setBytes(int index, ByteBuffer src) {
            int length = src.remaining();
            checkIndex(index, length);
            ByteBuffer tmp = internalNioBuffer();
            if (PlatformDependent.javaVersion() >= 16) {
                int offset = src.position();
                PlatformDependent.absolutePut(tmp, index, src, offset, length);
                src.position(offset + length);
            } else {
                tmp.position(index);
                tmp.put(src);
            }
            return this;
        }

        @Override
        public ByteBuf getBytes(int index, OutputStream out, int length)
                throws IOException {
            checkIndex(index, length);
            if (length != 0) {
                ByteBuffer tmp = internalNioBuffer();
                ByteBufUtil.readBytes(alloc(), tmp.hasArray() ? tmp : tmp.duplicate(), index, length, out);
            }
            return this;
        }

        @Override
        public int getBytes(int index, GatheringByteChannel out, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf);
        }

        @Override
        public int getBytes(int index, FileChannel out, long position, int length)
                throws IOException {
            ByteBuffer buf = internalNioBuffer().duplicate();
            buf.clear().position(index).limit(index + length);
            return out.write(buf, position);
        }

        @Override
        public int setBytes(int index, InputStream in, int length)
                throws IOException {
            checkIndex(index, length);
            final AbstractByteBuf rootParent = rootParent();
            if (rootParent.hasArray()) {
                return rootParent.setBytes(idx(index), in, length);
            }
            byte[] tmp = ByteBufUtil.threadLocalTempArray(length);
            int readBytes = in.read(tmp, 0, length);
            if (readBytes <= 0) {
                return readBytes;
            }
            setBytes(index, tmp, 0, readBytes);
            return readBytes;
        }

        @Override
        public int setBytes(int index, ScatteringByteChannel in, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length));
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setBytes(int index, FileChannel in, long position, int length)
                throws IOException {
            try {
                return in.read(internalNioBuffer(index, length), position);
            } catch (ClosedChannelException ignored) {
                return -1;
            }
        }

        @Override
        public int setCharSequence(int index, CharSequence sequence, Charset charset) {
            return setCharSequence0(index, sequence, charset, false);
        }

        private int setCharSequence0(int index, CharSequence sequence, Charset charset, boolean expand) {
            if (charset.equals(CharsetUtil.UTF_8)) {
                int length = ByteBufUtil.utf8MaxBytes(sequence);
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeUtf8(this, index, length, sequence, sequence.length());
            }
            if (charset.equals(CharsetUtil.US_ASCII) || charset.equals(CharsetUtil.ISO_8859_1)) {
                int length = sequence.length();
                if (expand) {
                    ensureWritable0(length);
                    checkIndex0(index, length);
                } else {
                    checkIndex(index, length);
                }
                return ByteBufUtil.writeAscii(this, index, sequence, length);
            }
            byte[] bytes = sequence.toString().getBytes(charset);
            if (expand) {
                ensureWritable0(bytes.length);
                // setBytes(...) will take care of checking the indices.
            }
            setBytes(index, bytes);
            return bytes.length;
        }

        @Override
        public int writeCharSequence(CharSequence sequence, Charset charset) {
            int written = setCharSequence0(writerIndex, sequence, charset, true);
            writerIndex += written;
            return written;
        }

        @Override
        public int forEachByte(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByte(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public int forEachByteDesc(int index, int length, ByteProcessor processor) {
            checkIndex(index, length);
            int ret = rootParent().forEachByteDesc(idx(index), length, processor);
            return forEachResult(ret);
        }

        @Override
        public ByteBuf setZero(int index, int length) {
            checkIndex(index, length);
            rootParent().setZero(idx(index), length);
            return this;
        }

        @Override
        public ByteBuf writeZero(int length) {
            ensureWritable(length);
            rootParent().setZero(idx(writerIndex), length);
            writerIndex += length;
            return this;
        }

        private int forEachResult(int ret) {
            if (ret < startIndex) {
                return -1;
            }
            return ret - startIndex;
        }

        @Override
        public boolean isContiguous() {
            return rootParent().isContiguous();
        }

        private int idx(int index) {
            return index + startIndex;
        }

        @Override
        protected void deallocate() {
            if (PlatformDependent.isJfrEnabled() && FreeBufferEvent.isEventEnabled()) {
                FreeBufferEvent event = new FreeBufferEvent();
                if (event.shouldCommit()) {
                    event.fill(this, AdaptiveByteBufAllocator.class);
                    event.commit();
                }
            }

            if (chunk != null) {
                chunk.releaseSegment(startIndex, maxFastCapacity);
            }
            tmpNioBuf = null;
            chunk = null;
            rootParent = null;
            handle.unguardedRecycle(this);
        }
    }

    /**
     * The strategy for how {@link AdaptivePoolingAllocator} should allocate chunk buffers.
     */
    interface ChunkAllocator {
        /**
         * Allocate a buffer for a chunk. This can be any kind of {@link AbstractByteBuf} implementation.
         * @param initialCapacity The initial capacity of the returned {@link AbstractByteBuf}.
         * @param maxCapacity The maximum capacity of the returned {@link AbstractByteBuf}.
         * @return The buffer that represents the chunk memory.
         */
        AbstractByteBuf allocate(int initialCapacity, int maxCapacity);
    }
}
