/*
 * Copyright 2013 The Netty Project
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
package io.netty.util;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.PlatformDependent.newFixedMpmcQueue;
import static io.netty.util.internal.PlatformDependent.newMpscQueue;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Light-weight object pool based on a thread-local stack.
 *
 * @param <T> the type of the pooled object
 */
public abstract class Recycler<T> {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Recycler.class);

    /**
     * We created this handle to avoid having more than 2 concrete implementations of {@link EnhancedHandle}
     * i.e. NOOP_HANDLE, {@link DefaultHandle} and the one used in the LocalPool.
     */
    private static final class LocalPoolHandle<T> extends EnhancedHandle<T> {
        private final UnguardedLocalPool<T> pool;

        private LocalPoolHandle(UnguardedLocalPool<T> pool) {
            this.pool = pool;
        }

        @Override
        public void recycle(T object) {
            UnguardedLocalPool<T> pool = this.pool;
            if (pool != null) {
                pool.release(object);
            }
        }

        @Override
        public void unguardedRecycle(final Object object) {
            UnguardedLocalPool<T> pool = this.pool;
            if (pool != null) {
                pool.release((T) object);
            }
        }
    }
    // 当某个对象不应当被回收或者回收功能被禁用时，系统会分配这个句柄给对象。当用户调用这个句柄的recycle()(回收)方法时，它什么都不做。避免
    // 了在代码中频繁的null值检查。
    private static final EnhancedHandle<?> NOOP_HANDLE = new LocalPoolHandle<>(null);
    // 容量为0的本地池,它用于在对象池被禁用或不可用的情况下作为占位符。既然容量为0，意味者无法存储任何对象，任何放入其中的对象都不会被立即丢弃。
    private static final UnguardedLocalPool<?> NOOP_LOCAL_POOL = new UnguardedLocalPool<>(0);
    // 定义了每个线程的对象池默认初始容量。 Netty的Recycler时基于threadLocal的，每个线程都有自己独立的对象池(栈)。
    private static final int DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD = 4 * 1024; // Use 4k instances as default.
    // 每个线程池
    private static final int DEFAULT_MAX_CAPACITY_PER_THREAD;
    // 采样比率,用于控制对象池的创建频率 由于Recycler是基于ThreadLocal的，如果创建过多的线程(如虚拟线程或大量普通线程),每个线程都维护一个池子
    // 可能会消耗过多内存。这个比率通常用于决定是否为某个线程分配真正的池子，或者使用容量为0的NOOP_LOCAL_POOL
    private static final int RATIO;
    // 定义了每个线程的池子内部队列的"块"大小
    private static final int DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD;
    // 标志位，用于指示是否使用"阻塞式"对象池
    private static final boolean BLOCKING_POOL;
    // 如果开启，可能意味着在某些批处理场景下，只使用FastThreadLocal优化的路径，而不回退到标准的JDK ThreadLocal，以获得极致的性能。
    private static final boolean BATCH_FAST_TL_ONLY;

    static {
        // In the future, we might have different maxCapacity for different object types.
        // e.g. io.netty.recycler.maxCapacity.writeTask
        //      io.netty.recycler.maxCapacity.outboundBuffer
        int maxCapacityPerThread = SystemPropertyUtil.getInt("io.netty.recycler.maxCapacityPerThread",
                SystemPropertyUtil.getInt("io.netty.recycler.maxCapacity", DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD));
        if (maxCapacityPerThread < 0) {
            maxCapacityPerThread = DEFAULT_INITIAL_MAX_CAPACITY_PER_THREAD;
        }

        DEFAULT_MAX_CAPACITY_PER_THREAD = maxCapacityPerThread;
        DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD = SystemPropertyUtil.getInt("io.netty.recycler.chunkSize", 32);

        // By default, we allow one push to a Recycler for each 8th try on handles that were never recycled before.
        // This should help to slowly increase the capacity of the recycler while not be too sensitive to allocation
        // bursts.
        // 每尝试回收8次，才真正允许1个对象进入池中
        // 为什么要这样做，1、避免过度缓存，高并发下，如果所有有用的对象都立刻放回池子，池子会瞬间爆满，且很多对象可能马上就不再被使用了。
        // 2、平滑扩容，这有助于让池子的容量缓慢增长，而不是对突发的分配请求过于敏感。
        RATIO = max(0, SystemPropertyUtil.getInt("io.netty.recycler.ratio", 8));
        // 控制当对象池满了或者获取对象时的行为。
        BLOCKING_POOL = SystemPropertyUtil.getBoolean("io.netty.recycler.blocking", false);

        BATCH_FAST_TL_ONLY = SystemPropertyUtil.getBoolean("io.netty.recycler.batchFastThreadLocalOnly", true);

        if (logger.isDebugEnabled()) {
            if (DEFAULT_MAX_CAPACITY_PER_THREAD == 0) {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: disabled");
                logger.debug("-Dio.netty.recycler.ratio: disabled");
                logger.debug("-Dio.netty.recycler.chunkSize: disabled");
                logger.debug("-Dio.netty.recycler.blocking: disabled");
                logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: disabled");
            } else {
                logger.debug("-Dio.netty.recycler.maxCapacityPerThread: {}", DEFAULT_MAX_CAPACITY_PER_THREAD);
                logger.debug("-Dio.netty.recycler.ratio: {}", RATIO);
                logger.debug("-Dio.netty.recycler.chunkSize: {}", DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
                logger.debug("-Dio.netty.recycler.blocking: {}", BLOCKING_POOL);
                logger.debug("-Dio.netty.recycler.batchFastThreadLocalOnly: {}", BATCH_FAST_TL_ONLY);
            }
        }
    }

    private final LocalPool<?, T> localPool;
    private final FastThreadLocal<LocalPool<?, T>> threadLocalPool;

    /**
     * USE IT CAREFULLY!<br>
     * This is creating a shareable {@link Recycler} which {@code get()} can be called concurrently from different
     * {@link Thread}s.<br>
     * Usually {@link Recycler}s uses some form of thread-local storage, but this constructor is disabling it
     * and using a single pool of instances instead, sized as {@code maxCapacity}<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded = true}:
     * it means that {@link Handle#recycle(Object)} is not checking that {@code object} is the same which was
     * recycled and assume no other recycling happens concurrently
     * (similar to what {@link EnhancedHandle#unguardedRecycle(Object)} does).<br>
     */
    // 小心使用，这个构造器创建的是一个可以被多个线程并发调用的共享Recycler，
    // UnguardedLocalPool 无保护模式，它在回收对象时不检查该对象是否真的属于这个池子，也不检查是否有其他线程正在并发修改。它假设调用者是
    // 诚实 且聪明的。
    // 风险 如果一个对象被错误地回收了两次，或者在不安全的情况下被回收，可能会导致数据损坏或程序崩溃。
    protected Recycler(int maxCapacity, boolean unguarded) {
        if (maxCapacity <= 0) {
            maxCapacity = 0;
        } else {
            maxCapacity = max(4, maxCapacity);
        }
        threadLocalPool = null;
        if (maxCapacity == 0) {
            localPool = (LocalPool<?, T>) NOOP_LOCAL_POOL;
        } else {
            localPool = unguarded? new UnguardedLocalPool<>(maxCapacity) : new GuardedLocalPool<>(maxCapacity);
        }
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded = true}:
     * it means that {@link Handle#recycle(Object)} is not checking that {@code object} is the same which was
     * recycled and assume no other recycling happens concurrently
     * (similar to what {@link EnhancedHandle#unguardedRecycle(Object)} does).<br>
     */
    protected Recycler(boolean unguarded) {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD, unguarded);
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded = true} as stated by
     * {@link #Recycler(boolean)} and allows to pin the recycler to a specific {@link Thread}, if {@code owner}
     * is not {@code null}.
     * <p>
     * Since this method has been introduced for performance-sensitive cases it doesn't validate if {@link #get()} is
     * called from the {@code owner} {@link Thread}: it assumes {@link #get()} to never happen concurrently.
     * <p>
     */
    protected Recycler(Thread owner, boolean unguarded) {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD, owner, unguarded);
    }

    protected Recycler(int maxCapacityPerThread) {
        this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * 大部分使用的都是这个无参构造方法
     * 最终调用的构造方法是private Recycler(int maxCapacityPerThread, int ratio, int chunkSize, boolean useThreadLocalStorage,
     *                      Thread owner, boolean unguarded) {
     *    具体说明查看最终调用的构造方法
     *
     */
    protected Recycler() {
        this(DEFAULT_MAX_CAPACITY_PER_THREAD);
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded = true} as stated by
     * {@link #Recycler(boolean)}, but it allows to tune the chunk size used for local pooling.
     */
    protected Recycler(int chunksSize, int maxCapacityPerThread, boolean unguarded) {
        this(maxCapacityPerThread, RATIO, chunksSize, unguarded);
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded = true} and allows pinning
     * the recycler to a specific {@link Thread}, as stated by {@link #Recycler(Thread, boolean)}.<br>
     * It also allows tuning the chunk size used for local pooling and the max capacity per thread.
     *
     * @throws IllegalArgumentException if {@code owner} is {@code null}.
     */
    protected Recycler(int chunkSize, int maxCapacityPerThread, Thread owner, boolean unguarded) {
        this(maxCapacityPerThread, RATIO, chunkSize, owner, unguarded);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor) {
        this(maxCapacityPerThread, RATIO, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread) {
        this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    /**
     * @deprecated Use one of the following instead:
     * {@link #Recycler()}, {@link #Recycler(int)}, {@link #Recycler(int, int, int)}.
     */
    @Deprecated
    @SuppressWarnings("unused") // Parameters we can't remove due to compatibility.
    protected Recycler(int maxCapacityPerThread, int maxSharedCapacityFactor,
                       int ratio, int maxDelayedQueuesPerThread, int delayedQueueRatio) {
        this(maxCapacityPerThread, ratio, DEFAULT_QUEUE_CHUNK_SIZE_PER_THREAD);
    }

    protected Recycler(int maxCapacityPerThread, int interval, int chunkSize) {
        this(maxCapacityPerThread, interval, chunkSize, true, null, false);
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded =true}
     * as stated by {@link #Recycler(boolean)}.
     */
    protected Recycler(int maxCapacityPerThread, int interval, int chunkSize, boolean unguarded) {
        this(maxCapacityPerThread, interval, chunkSize, true, null, unguarded);
    }

    /**
     * USE IT CAREFULLY!<br>
     * This is NOT enforcing pooled instances states to be validated if {@code unguarded =true}
     * as stated by {@link #Recycler(boolean)}.
     */
    protected Recycler(int maxCapacityPerThread, int interval, int chunkSize, Thread owner, boolean unguarded) {
        this(maxCapacityPerThread, interval, chunkSize, false, owner, unguarded);
    }

    /**
     * useThreadLocalStorage 大部分情况下是调用的无参构造方法，调到这里时这个变量时true
     *  owner是null
     *  maxCapacityPerThread 默认值 大于0
     *  两种使用模式
     *  1、多线程共享Recycler实例 内部应该使用的是threadLocalPool 每个线程有自己的LocalPool 操作LocalPool 无并发问题
     *  2、单线程使用Recycler实例，内部应该使用的是LocalPool，但是owner为指定的线程。
     *  不能说单线程使用，但是owner不给值
     *  也不能说多线程共享Recycler，但是条件走的是使用LocalPool
     *
     *
     */

    @SuppressWarnings("unchecked")
    private Recycler(int maxCapacityPerThread, int ratio, int chunkSize, boolean useThreadLocalStorage,
                     Thread owner, boolean unguarded) {
        final int interval = max(0, ratio);
        if (maxCapacityPerThread <= 0) {
            maxCapacityPerThread = 0;
            chunkSize = 0;
        } else {
            maxCapacityPerThread = max(4, maxCapacityPerThread);
            chunkSize = max(2, min(chunkSize, maxCapacityPerThread >> 1));
        }
        if (maxCapacityPerThread > 0 && useThreadLocalStorage) {
            final int finalMaxCapacityPerThread = maxCapacityPerThread;
            final int finalChunkSize = chunkSize;
            threadLocalPool = new FastThreadLocal<LocalPool<?, T>>() {
                @Override
                protected LocalPool<?, T> initialValue() {
                    return unguarded? new UnguardedLocalPool<>(finalMaxCapacityPerThread, interval, finalChunkSize) :
                            new GuardedLocalPool<>(finalMaxCapacityPerThread, interval, finalChunkSize);
                }

                @Override
                protected void onRemoval(LocalPool<?, T> value) throws Exception {
                    super.onRemoval(value);
                    MessagePassingQueue<?> handles = value.pooledHandles;
                    value.pooledHandles = null;
                    value.owner = null;
                    if (handles != null) {
                        handles.clear();
                    }
                }
            };
            localPool = null;
        } else {
            threadLocalPool = null;
            if (maxCapacityPerThread == 0) {
                localPool = (LocalPool<?, T>) NOOP_LOCAL_POOL;
            } else {
                Objects.requireNonNull(owner, "owner");
                localPool = unguarded? new UnguardedLocalPool<>(owner, maxCapacityPerThread, interval, chunkSize) :
                        new GuardedLocalPool<>(owner, maxCapacityPerThread, interval, chunkSize);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public final T get() {
        // 如果这个localPool不为空，说明是单线程独占这个Recycler. 单线程使用的时候用户一定要在指定的线程中调用get方法，
        // 不能说Recycler是线程A的，要在B线程使用这个Recycler对象。
        if (localPool != null) {
            return localPool.getWith(this);
        } else {
            if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
                return newObject((Handle<T>) NOOP_HANDLE);
            }
            return threadLocalPool.get().getWith(this);
        }
    }

    /**
     * Disassociates the {@link Recycler} from the current {@link Thread} if it was pinned,
     * see {@link #Recycler(Thread, boolean)}.
     * <p>
     * Be aware that this method is not thread-safe: it's necessary to allow a {@link Thread} to
     * be garbage collected even if {@link Handle}s are still referenced by other objects.
     * <p>
     */
    public static void unpinOwner(Recycler<?> recycler) {
        if (recycler.localPool != null) {
            recycler.localPool.owner = null;
        }
    }

    /**
     * @deprecated use {@link Handle#recycle(Object)}.
     */
    @Deprecated
    public final boolean recycle(T o, Handle<T> handle) {
        if (handle == NOOP_HANDLE) {
            return false;
        }

        handle.recycle(o);
        return true;
    }

    @VisibleForTesting
    final int threadLocalSize() {
        if (localPool != null) {
            return localPool.size();
        } else {
            if (!FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()) {
                return 0;
            }
            final LocalPool<?, T> pool = threadLocalPool.getIfExists();
            if (pool == null) {
                return 0;
            }
            return pool.size();
        }
    }

    /**
     * @param handle can NOT be null.
     */
    protected abstract T newObject(Handle<T> handle);

    @SuppressWarnings("ClassNameSameAsAncestorName") // Can't change this due to compatibility.
    public interface Handle<T> extends ObjectPool.Handle<T>  { }

    @UnstableApi
    public abstract static class EnhancedHandle<T> implements Handle<T> {

        public abstract void unguardedRecycle(Object object);

        private EnhancedHandle() {
        }
    }

    private static final class DefaultHandle<T> extends EnhancedHandle<T> {
        private static final int STATE_CLAIMED = 0;
        private static final int STATE_AVAILABLE = 1;
        private static final AtomicIntegerFieldUpdater<DefaultHandle<?>> STATE_UPDATER;
        static {
            AtomicIntegerFieldUpdater<?> updater = AtomicIntegerFieldUpdater.newUpdater(DefaultHandle.class, "state");
            //noinspection unchecked
            STATE_UPDATER = (AtomicIntegerFieldUpdater<DefaultHandle<?>>) updater;
        }

        private volatile int state; // State is initialised to STATE_CLAIMED (aka. 0) so they can be released.
        private final GuardedLocalPool<T> localPool;
        private T value;

        DefaultHandle(GuardedLocalPool<T> localPool) {
            this.localPool = localPool;
        }

        @Override
        public void recycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }
            toAvailable();
            localPool.release(this);
        }

        @Override
        public void unguardedRecycle(Object object) {
            if (object != value) {
                throw new IllegalArgumentException("object does not belong to handle");
            }
            unguardedToAvailable();
            localPool.release(this);
        }

        T claim() {
            assert state == STATE_AVAILABLE;
            STATE_UPDATER.lazySet(this, STATE_CLAIMED);
            return value;
        }

        void set(T value) {
            this.value = value;
        }

        private void toAvailable() {
            int prev = STATE_UPDATER.getAndSet(this, STATE_AVAILABLE);
            if (prev == STATE_AVAILABLE) {
                throw new IllegalStateException("Object has been recycled already.");
            }
        }

        private void unguardedToAvailable() {
            int prev = state;
            if (prev == STATE_AVAILABLE) {
                throw new IllegalStateException("Object has been recycled already.");
            }
            STATE_UPDATER.lazySet(this, STATE_AVAILABLE);
        }
    }

    private static final class GuardedLocalPool<T> extends LocalPool<DefaultHandle<T>, T> {

        GuardedLocalPool(int maxCapacity) {
            super(maxCapacity);
        }

        GuardedLocalPool(Thread owner, int maxCapacity, int ratioInterval, int chunkSize) {
            super(owner, maxCapacity, ratioInterval, chunkSize);
        }

        GuardedLocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
            super(maxCapacity, ratioInterval, chunkSize);
        }

        @Override
        public T getWith(Recycler<T> recycler) {
            DefaultHandle<T> handle = acquire();
            T obj;
            if (handle == null) {
                handle = canAllocatePooled()? new DefaultHandle<>(this) : null;
                if (handle != null) {
                    obj = recycler.newObject(handle);
                    handle.set(obj);
                } else {
                    obj = recycler.newObject((Handle<T>) NOOP_HANDLE);
                }
            } else {
                obj = handle.claim();
            }
            return obj;
        }
    }

    private static final class UnguardedLocalPool<T> extends LocalPool<T, T> {
        private final EnhancedHandle<T> handle;

        UnguardedLocalPool(int maxCapacity) {
            super(maxCapacity);
            handle = maxCapacity == 0? null : new LocalPoolHandle<>(this);
        }

        UnguardedLocalPool(Thread owner, int maxCapacity, int ratioInterval, int chunkSize) {
            super(owner, maxCapacity, ratioInterval, chunkSize);
            handle = new LocalPoolHandle<>(this);
        }

        UnguardedLocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
            super(maxCapacity, ratioInterval, chunkSize);
            handle = new LocalPoolHandle<>(this);
        }

        @Override
        public T getWith(Recycler<T> recycler) {
            T obj = acquire();
            if (obj == null) {
                obj = recycler.newObject(canAllocatePooled()? handle : (Handle<T>) NOOP_HANDLE);
            }
            return obj;
        }
    }

    private abstract static class LocalPool<H, T> {
        private final int ratioInterval;
        // 这是一个临时缓冲区(数组)。当从池中获取对象时，为了提高效率，Netty会一次性从底层队列pooledHandles中抓取一批对象
        // 比如抓取32个,存入这个batch数组中。下次再要对象时，直接从数组里取，避免频繁操作并发队列。
        private final H[] batch;
        // 记录当前batch数据里还剩余多少个可用对象。
        private int batchSize;
        // 标记这个池子是属于哪个线程。如果是线程本地池，owner就是当前线程；如果是共享池，owner可能是null。这用于判断是否需要加锁或进行线程安全检查
        private Thread owner;
        // 核心队列，这是真正的"仓库"，所有被回收的句柄最终都会存储在这个队列中
        private MessagePassingQueue<H> pooledHandles;
        // 计数器，配合ratioInterval使用。每调用一次回收，计数器加1，当计数器达到ratioInterval时，触发真正的入队操作并重置计数器。
        private int ratioCounter;

        LocalPool(int maxCapacity) {
            // if there's no capacity, we need to never allocate pooled objects.
            // if there's capacity, because there is a shared pool, we always pool them, since we cannot trust the
            // thread unsafe ratio counter.
            this.ratioInterval = maxCapacity == 0? -1 : 0;
            this.owner = null;
            batch = null;
            batchSize = 0;
            pooledHandles = createExternalMcPool(maxCapacity);
            ratioCounter = 0;
        }

        @SuppressWarnings("unchecked")
        LocalPool(Thread owner, int maxCapacity, int ratioInterval, int chunkSize) {
            this.ratioInterval = ratioInterval;
            this.owner = owner;
            batch = owner != null? (H[]) new Object[chunkSize] : null;
            batchSize = 0;
            pooledHandles = createExternalScPool(chunkSize, maxCapacity);
            ratioCounter = ratioInterval; // Start at interval so the first one will be recycled.
        }

        private static <H> MessagePassingQueue<H> createExternalMcPool(int maxCapacity) {
            if (maxCapacity == 0) {
                return null;
            }
            if (BLOCKING_POOL) {
                return new BlockingMessageQueue<>(maxCapacity);
            }
            return (MessagePassingQueue<H>) newFixedMpmcQueue(maxCapacity);
        }

        private static <H> MessagePassingQueue<H> createExternalScPool(int chunkSize, int maxCapacity) {
            if (maxCapacity == 0) {
                return null;
            }
            if (BLOCKING_POOL) {
                return new BlockingMessageQueue<>(maxCapacity);
            }
            return (MessagePassingQueue<H>) newMpscQueue(chunkSize, maxCapacity);
        }

        LocalPool(int maxCapacity, int ratioInterval, int chunkSize) {
            this(!BATCH_FAST_TL_ONLY || FastThreadLocalThread.currentThreadWillCleanupFastThreadLocals()
                         ? Thread.currentThread() : null, maxCapacity, ratioInterval, chunkSize);
        }

        // 这个方法不会存在并发 因为当前的LocalPool一定是属于某一个线程的。具体查看Recycler构造方法上的注释
        // LocalPool本身不是线程安全的，但它的访问被严格限制在单线程，要么是threadLocal模式下的调用线程自己的本地
        // 要么是localPool模式下的 绑定的owner的线程。
        protected final H acquire() {
            // 先判断当前数组是不是为空。
            int size = batchSize;
            if (size == 0) {
                // 这里按道理LocalPool是线程独占的，pooledHandles也应该是独占的，为什么会有并发问题呢，这里的并发问题不是acquire()方法被多线程
                // 调用,而是指pooledHandles被多线程操作，那谁在操作呢，有两类线程操作，1、独占当前LocalPool的线程，2 其他线程，主要在执行
                // release操作时，因为不是独占的那个线程，会把对象relaxedOffer到这个队列。 还有一个线程销毁逻辑中onRemoval会把pooledHandles
                // 清空并赋值为null
                // 当其他线程把pooledHandles赋值为null时，独占的那个线程还未看到，即便读取到旧对立的引用，relaxedPoll 最多返回空或者
                // 复用一个不会再回收的对象，不影响程序的正确性
                // 为什么会有"复用一个不会再会回收的对象"这种对象
                // 假设polledHandles = null，还未来得及handles.clear，独占的那个线程在这个间隙读到了旧的队列引用，调用relaxedPoll()
                // 拿到了队列的一个对象， 队列别销毁了，这个对象没有办法再放回队列了，可以回到batch，但是回到batch的条件时owner不为空，因为队列被销毁时
                // 先执行polledHandles = null 再执行owner=null，所以没办法回batch了也。
                // 如果再owner=null 之前就release，还是会回到batch中的，那么就正在复用就行了。
                // it's ok to be racy; at worst we reuse something that won't return back to the pool
                final MessagePassingQueue<H> handles = pooledHandles;
                if (handles == null) {
                    return null;
                }
                return handles.relaxedPoll();
            }
            // 数组不为空，获取数组最后的元素
            int top = size - 1;
            final H h = batch[top];
            batchSize = top;
            batch[top] = null;
            return h;
        }

        protected final void release(H handle) {
            // 能回到batch的条件一定时owner线程不为空，且当前线程为owner。
            Thread owner = this.owner;
            if (owner != null && Thread.currentThread() == owner && batchSize < batch.length) {
                batch[batchSize] = handle;
                batchSize++;
                // owner不为空，但是owner已经Terminated状态。
            } else if (owner != null && isTerminated(owner)) {
                pooledHandles = null;
                this.owner = null;
            } else {
                // 如果a线程，也是owner线程 调用release方法，进入到(owner != null && isTerminated(owner)) 这个逻辑了，将
                // owner = null 那此时另一个线程b调用release方法，进来就会判断owner是null，进入到当前这个分支，然后会将对象释放到
                // pooledHandles这个线程。
                MessagePassingQueue<H> handles = pooledHandles;
                if (handles != null) {
                    handles.relaxedOffer(handle);
                }
            }
        }

        private static boolean isTerminated(Thread owner) {
            // Do not use `Thread.getState()` in J9 JVM because it's known to have a performance issue.
            // See: https://github.com/netty/netty/issues/13347#issuecomment-1518537895
            return PlatformDependent.isJ9Jvm()? !owner.isAlive() : owner.getState() == Thread.State.TERMINATED;
        }

        boolean canAllocatePooled() {
            if (ratioInterval < 0) {
                return false;
            }
            if (ratioInterval == 0) {
                return true;
            }
            if (++ratioCounter >= ratioInterval) {
                ratioCounter = 0;
                return true;
            }
            return false;
        }

        abstract T getWith(Recycler<T> recycler);

        int size() {
            MessagePassingQueue<H> handles = pooledHandles;
            final int externalSize = handles != null? handles.size() : 0;
            return externalSize + (batch != null? batchSize : 0);
        }
    }

    /**
     * This is an implementation of {@link MessagePassingQueue}, similar to what might be returned from
     * {@link PlatformDependent#newMpscQueue(int)}, but intended to be used for debugging purpose.
     * The implementation relies on synchronised monitor locks for thread-safety.
     * The {@code fill} bulk operation is not supported by this implementation.
     */
    private static final class BlockingMessageQueue<T> implements MessagePassingQueue<T> {
        private final Queue<T> deque;
        private final int maxCapacity;

        BlockingMessageQueue(int maxCapacity) {
            this.maxCapacity = maxCapacity;
            // This message passing queue is backed by an ArrayDeque instance,
            // made thread-safe by synchronising on `this` BlockingMessageQueue instance.
            // Why ArrayDeque?
            // We use ArrayDeque instead of LinkedList or LinkedBlockingQueue because it's more space efficient.
            // We use ArrayDeque instead of ArrayList because we need the queue APIs.
            // We use ArrayDeque instead of ConcurrentLinkedQueue because CLQ is unbounded and has O(n) size().
            // We use ArrayDeque instead of ArrayBlockingQueue because ABQ allocates its max capacity up-front,
            // and these queues will usually have large capacities, in potentially great numbers (one per thread),
            // but often only have comparatively few items in them.
            deque = new ArrayDeque<T>();
        }

        @Override
        public synchronized boolean offer(T e) {
            if (deque.size() == maxCapacity) {
                return false;
            }
            return deque.offer(e);
        }

        @Override
        public synchronized T poll() {
            return deque.poll();
        }

        @Override
        public synchronized T peek() {
            return deque.peek();
        }

        @Override
        public synchronized int size() {
            return deque.size();
        }

        @Override
        public synchronized void clear() {
            deque.clear();
        }

        @Override
        public synchronized boolean isEmpty() {
            return deque.isEmpty();
        }

        @Override
        public int capacity() {
            return maxCapacity;
        }

        @Override
        public boolean relaxedOffer(T e) {
            return offer(e);
        }

        @Override
        public T relaxedPoll() {
            return poll();
        }

        @Override
        public T relaxedPeek() {
            return peek();
        }

        @Override
        public int drain(Consumer<T> c, int limit) {
            T obj;
            int i = 0;
            for (; i < limit && (obj = poll()) != null; i++) {
                c.accept(obj);
            }
            return i;
        }

        @Override
        public int fill(Supplier<T> s, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drain(Consumer<T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int fill(Supplier<T> s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drain(Consumer<T> c, WaitStrategy wait, ExitCondition exit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fill(Supplier<T> s, WaitStrategy wait, ExitCondition exit) {
            throw new UnsupportedOperationException();
        }
    }
}
