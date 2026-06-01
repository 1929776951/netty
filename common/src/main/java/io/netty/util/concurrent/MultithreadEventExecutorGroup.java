/*
 * Copyright 2012 The Netty Project
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
package io.netty.util.concurrent;

import io.netty.util.concurrent.AutoScalingEventExecutorChooserFactory.AutoScalingUtilizationMetric;
import io.netty.util.concurrent.EventExecutorChooserFactory.ObservableEventExecutorChooser;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.util.internal.ObjectUtil.checkPositive;

/**
 * Abstract base class for {@link EventExecutorGroup} implementations that handles their tasks with multiple threads at
 * the same time.
 */
public abstract class MultithreadEventExecutorGroup extends AbstractEventExecutorGroup {

    private final EventExecutor[] children;
    private final Set<EventExecutor> readonlyChildren;
    private final AtomicInteger terminatedChildren = new AtomicInteger();
    private final Promise<?> terminationFuture = new DefaultPromise(GlobalEventExecutor.INSTANCE);
    private final EventExecutorChooserFactory.EventExecutorChooser chooser;

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param threadFactory     the ThreadFactory to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, ThreadFactory threadFactory, Object... args) {
        this(nThreads, threadFactory == null ? null : new ThreadPerTaskExecutor(threadFactory), args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    // 这个构造器中的入参Executor 是Netty用来创建和管理EventExecutor线程的底层执行器
    // 1、提供线程执行的底层能力 它本质是一个java.util.concurrent.Executor 负责创建、调度和执行线程任务。这个线程池默认的实现
    // 是ThreadPerTaskExecutor，这个线程池没有那么多负责功能，即我们常用的核心线程，最大线程，等待队列等。这个线程池的执行方法就是
    // 调用这个ThreadPerTaskExecutor的executor方法启动一个新线程。是不是这里跟理解的不太一样，不是说一个EventLoop一个线程吗。
    // 其实是每一个EventLoop在第一次提交任务时，调用这个的executor，启动了一个线程，这个线程会死循环，整个eventLoop生命周期内不结束
    // 再进一步 真实的EventLoop在启动任务时，还做了两部操作，一个是为了executor时传入EventLoop，另一个是在执行任务前往fastThreadLocal
    // 里面加入eventLoop finally里面也加入eventLoop
    // 具体查看 ThreadExecutorMap.apply(executor, this);
    // 再进一步说明一下，启动eventLoop的线程的不是ThreadPerTaskExecutor，而是一个Executor的匿名内部类对象，里面转调了ThreadPerTaskExecutor实例的
    // executor，这一步传入了eventLoop对象 ，然后再执行任务的时候，将EventLoop设置到当前线程的fastThreadLocal，执行完后又设置了一次，我感觉这个线程完的时候
    // 也就是eventLoop对象完结的时候 既然这个finally在销毁时才执行，也就是永远不会执行，除非这个eventLoop结束了
    // 这里是个通用的方法，开发人员可能会调用apply方法这么执行，因为方法入参需要EventExecutor对象，我们开发人员谁会自己new一个这个对象呢
    // 什么场景下用呢，DefaultEventExecutorGroup 自定义业务线程池时，执行短任务时会使用，我们自定义的DefaultEventExecutorGroup这个里面
    // 执行任务时，启动的任务可能不会是死循环的，所以这下应该清楚了
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor, Object... args) {
        this(nThreads, executor, DefaultEventExecutorChooserFactory.INSTANCE, args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param chooserFactory    the {@link EventExecutorChooserFactory} to use.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor,
                                            EventExecutorChooserFactory chooserFactory, Object... args) {
        checkPositive(nThreads, "nThreads");

        if (executor == null) {
            executor = new ThreadPerTaskExecutor(newDefaultThreadFactory());
        }

        children = new EventExecutor[nThreads];

        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                children[i] = newChild(executor, args);
                success = true;
            } catch (Exception e) {
                // TODO: Think about if this is a good exception type
                throw new IllegalStateException("failed to create a child event loop", e);
            } finally {
                if (!success) {
                    for (int j = 0; j < i; j ++) {
                        children[j].shutdownGracefully();
                    }

                    for (int j = 0; j < i; j ++) {
                        EventExecutor e = children[j];
                        try {
                            while (!e.isTerminated()) {
                                e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException interrupted) {
                            // Let the caller handle the interruption.
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }

        chooser = chooserFactory.newChooser(children);

        final FutureListener<Object> terminationListener = future -> {
            if (terminatedChildren.incrementAndGet() == children.length) {
                terminationFuture.setSuccess(null);
            }
        };

        for (EventExecutor e: children) {
            e.terminationFuture().addListener(terminationListener);
        }

        Set<EventExecutor> childrenSet = new LinkedHashSet<EventExecutor>(children.length);
        Collections.addAll(childrenSet, children);
        readonlyChildren = Collections.unmodifiableSet(childrenSet);
    }

    protected ThreadFactory newDefaultThreadFactory() {
        return new DefaultThreadFactory(getClass());
    }

    @Override
    public EventExecutor next() {
        return chooser.next();
    }

    @Override
    public Iterator<EventExecutor> iterator() {
        return readonlyChildren.iterator();
    }

    /**
     * Return the number of {@link EventExecutor} this implementation uses. This number is the maps
     * 1:1 to the threads it use.
     */
    public final int executorCount() {
        return children.length;
    }

    /**
     * Returns the number of currently active threads if the group is using an
     * {@link ObservableEventExecutorChooser}. Otherwise, for a non-scaling group,
     * this method returns the total number of threads, as all are considered active.
     *
     * @return the count of active threads.
     */
    public int activeExecutorCount() {
        if (chooser instanceof ObservableEventExecutorChooser) {
            return ((ObservableEventExecutorChooser) chooser).activeExecutorCount();
        }
        return executorCount();
    }

    /**
     * Returns a list of real-time utilization metrics if the group was configured
     * with a compatible {@link EventExecutorChooserFactory}, otherwise an empty list.
     *
     * @return A list of {@link AutoScalingUtilizationMetric} objects.
     */
    public List<AutoScalingUtilizationMetric> executorUtilizations() {
        if (chooser instanceof ObservableEventExecutorChooser) {
            return ((ObservableEventExecutorChooser) chooser).executorUtilizations();
        }
        return Collections.emptyList();
    }

    /**
     * Create a new EventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultithreadEventExecutorGroup}.
     *
     */
    protected abstract EventExecutor newChild(Executor executor, Object... args) throws Exception;

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        for (EventExecutor l: children) {
            l.shutdownGracefully(quietPeriod, timeout, unit);
        }
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        for (EventExecutor l: children) {
            l.shutdown();
        }
    }

    @Override
    public boolean isShuttingDown() {
        for (EventExecutor l: children) {
            if (!l.isShuttingDown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isShutdown() {
        for (EventExecutor l: children) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (EventExecutor l: children) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        loop: for (EventExecutor l: children) {
            for (;;) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) {
                    break loop;
                }
                if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
        }
        return isTerminated();
    }
}
