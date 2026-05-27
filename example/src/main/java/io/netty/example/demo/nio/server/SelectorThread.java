package io.netty.example.demo.nio.server;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class SelectorThread extends Thread {

    private final Selector selector;
    private final Queue<SocketChannel> acceptedQueue;

    public SelectorThread() throws IOException {
        setDaemon(true);
        this.selector = Selector.open();
        this.acceptedQueue = new LinkedList<SocketChannel>();
    }

    @Override
    public void run() {

        while (true) {
            try {
                // 最开始这个线程启动的时候，当前的selector上还未关注任何事件,acceptedQueue和updateQueue队列中是没有任何
                // 数据的，先调用select()，是为了不让这个while(true)空转，先通过select()方法阻塞住。直到有事件发生。
                select();
                processAcceptedConnections();
            } catch (RuntimeException e) {
                System.out.println("Ignoring unexpected runtime exception");
            } catch (Exception e) {
                System.out.println("Ignoring unexpected exception");
            }
        }


    }

    private void processAcceptedConnections() {
        SocketChannel accepted;
        while ((accepted = acceptedQueue.poll()) != null) {
            SelectionKey key = null;
            try {
                key = accepted.register(selector, SelectionKey.OP_READ);
                // 这里可以通过key.attach(xxx); 方法给当前的selectionKey上绑定一个业务对象
                // 方便后续在当前selectionKey上有事件时，可以快速获取对应的业务对象，然后做处理
            } catch (IOException e) {
                if (key != null) {
                    try {
                        key.cancel();
                    } catch (Exception ex) {
                        System.out.println("ignoring exception during selectionkey cancel");
                    }
                }
            }
            try {
                accepted.close();
            } catch (IOException e) {
                System.out.println("ignoring exception during close");
            }
        }
    }


    private void select() {
        try {
            selector.select();
            Set<SelectionKey> selected = selector.selectedKeys();
            ArrayList<SelectionKey> selectedList =
                    new ArrayList<SelectionKey>(selected);
            Collections.shuffle(selectedList);
            Iterator<SelectionKey> selectedKeys = selectedList.iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selected.remove(key);
                if (!key.isValid()) {
                    if (key != null) {
                        try {
                            key.cancel();
                        } catch (Exception ex) {
                            System.out.println("ignoring exception during selectionkey cancel");
                        }
                    }
                    continue;
                }
                if (key.isReadable() || key.isWritable()) {
                    handleIO(key);
                } else {
                    System.out.println("Unexpected ops in select " + key.readyOps());
                }
            }
        } catch (IOException e) {
            System.out.println("Ignoring IOException while selecting");
        }
    }

    private void handleIO(SelectionKey key) {
        if (!key.isValid()) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    System.out.println("ignoring exception during selectionkey cancel");
                }
            }
            return;
        }
        if (key.isReadable() || key.isWritable()) {
            SelectableChannel channel = key.channel();
            if(!channel.isOpen()){
                return;
            }
            // 这里可写事件 的注册要讲究，如果注册的写事件，那么发送缓冲区有空间就会触发，会很频繁，最好的处理时，在真的有需要的写的数据
            // 的时候注册写事件  比如先读，读到数据后，业务上如果产生了需要回复数据，放入发送队列，并开启OP_WRITE事件. 并且在处理写事件
            // 完成后，一定要通过key.interestOps(key.interestOps& ~OP_WRITE)方法关闭写监听的注册。

        }
    }


    public boolean addAcceptedConnection(SocketChannel accepted) {
        if (!acceptedQueue.offer(accepted)) {
            return false;
        }
        // 因为当前线程的run方法可能在select()上阻塞这，拿到一个新连接后，wakeUp一下，让run方法能及时调用processAcceptedConnections()方法
        // 也即run方法中select()方法不再阻塞，能及时处理新连接
        wakeupSelector();
        return true;
    }

    public void wakeupSelector() {
        // wakeup方法使得尚未返回的select()方法立即返回，如果当前没有阻塞在select()上,则下一次调用select()将立即返回
        selector.wakeup();
    }


}
