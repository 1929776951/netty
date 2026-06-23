package io.netty.example.demo.jdkstudy.threadpoolexecutor;


import java.lang.reflect.Field;
import java.util.concurrent.*;

public class MyThreadPoolExecutor extends ThreadPoolExecutor {

    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    /**
     *  每个任务执行之前的前置执行
     * @param t the thread that will run task {@code r}
     * @param r the task that will be executed
     */
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        Object originalTask = TaskUnWrapUtil.getOriginalTask(r);
        if(originalTask instanceof MyRunnable) {
            MyRunnable myRunnable  = (MyRunnable)originalTask;
            String businessUniqueIdentifier = myRunnable.getBusinessUniqueIdentifier();
            System.out.println("beforeExecute执行。。。,businessUniqueIdentifier为:"+businessUniqueIdentifier);
        } else {
            System.out.println("beforeExecute执行。。。,线程不是MyRunnable类型");
        }

    }

    /**
     *  每个任务执行完成后后置执行
     * @param r the runnable that has completed
     * @param t the exception that caused termination, or null if
     * execution completed normally
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        System.out.println("afterExecute执行任务的线程为:"+Thread.currentThread().getName());
    }

    static class TaskUnWrapUtil{
        private static final Field FUTURE_TASK_CALLABLE_FIELD;
        private static final Field RUNNABLE_ADAPTER_TASK_FIELD;
        private static final Field ASYNC_RUN_FN_FIELD;
        static{
            try {
                FUTURE_TASK_CALLABLE_FIELD = FutureTask.class.getDeclaredField("callable");
                FUTURE_TASK_CALLABLE_FIELD.setAccessible(true);
                RUNNABLE_ADAPTER_TASK_FIELD
                        = Class.forName("java.util.concurrent.Executors$RunnableAdapter")
                        .getDeclaredField("task");
                RUNNABLE_ADAPTER_TASK_FIELD.setAccessible(true);
                ASYNC_RUN_FN_FIELD = Class.forName("java.util.concurrent.CompletableFuture$AsyncRun")
                        .getDeclaredField("fn");
                ASYNC_RUN_FN_FIELD.setAccessible(true);
            } catch (NoSuchFieldException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public static Object getOriginalTask(Runnable r){
            // CompletableFuture的
            if(r.getClass().getName().equals("java.util.concurrent.CompletableFuture$AsyncRun")){
                try {
                    return ASYNC_RUN_FN_FIELD.get(r);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            // 普通FutureTask
            if(r instanceof FutureTask){
                try {

                    Object callable =  FUTURE_TASK_CALLABLE_FIELD.get(r);
                    if(callable.getClass().getName().equals("java.util.concurrent.Executors$RunnableAdapter")){
                        return RUNNABLE_ADAPTER_TASK_FIELD.get(callable);
                    }
                    return callable;
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            return r;

        }
    }
}
