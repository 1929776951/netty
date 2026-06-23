package io.netty.example.demo.jdkstudy.threadpoolexecutor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorDemo {


    public static void main(String[] args) {
        ExecutorService executorService = new MyThreadPoolExecutor(5,10,
                300, TimeUnit.SECONDS,new ArrayBlockingQueue<>(10000));
        executorService.submit(new MyRunnable("任务1"));
//        executorService.submit(new MyRunnable("任务2"));
//        executorService.submit(new Callable<Void>() {
//            @Override
//            public Void call() throws Exception {
//                return null;
//            }
//        });
        executorService.shutdown();
    }

}
