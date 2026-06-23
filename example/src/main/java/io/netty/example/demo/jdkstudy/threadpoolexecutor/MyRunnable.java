package io.netty.example.demo.jdkstudy.threadpoolexecutor;

public class MyRunnable extends ParentRunnable {



    @Override
    public void run() {
        System.out.println("run方法执行---任务："+idStr+"运行,任务业务:"+getBusinessUniqueIdentifier());
    }

    public MyRunnable(String businessUniqueIdentifier) {
        setBusinessUniqueIdentifier(businessUniqueIdentifier);
    }


}
