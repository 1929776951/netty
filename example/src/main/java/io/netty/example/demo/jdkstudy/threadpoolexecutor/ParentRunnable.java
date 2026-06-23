package io.netty.example.demo.jdkstudy.threadpoolexecutor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ParentRunnable implements Runnable {

    private final static AtomicLong id = new AtomicLong();

    protected String idStr = String.valueOf(id.getAndIncrement());

    protected String businessUniqueIdentifier;



    public String getIdStr() {
        return idStr;
    }

    public void setIdStr(String idStr) {
        this.idStr = idStr;
    }

    public String getBusinessUniqueIdentifier() {
        return businessUniqueIdentifier;
    }

    public void setBusinessUniqueIdentifier(String businessUniqueIdentifier) {
        this.businessUniqueIdentifier = businessUniqueIdentifier;
    }

}
