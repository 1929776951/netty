package io.netty.example.demo.nio.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.Set;

public class NioServer {

    private static final int PORT = 8866;

    private static final int BUF_SIZE = 1024;

    public static void main(String[] args) {

        try {
            int numCores = Runtime.getRuntime().availableProcessors();
            int numSelectorThreads = Math.max((int) Math.sqrt((float) numCores/2), 1);
            Set<SelectorThread> selectorThreads =
                    new HashSet<SelectorThread>();
            for(int i=0; i<numSelectorThreads; ++i) {
                selectorThreads.add(new SelectorThread());
            }
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            // 设置读取操作的超时时间，当调用accept(),read()时,若在指定时间内无数据到达或无连接请求，将抛出SocketTimeoutException
            // 设置为0，表示无限等待(阻塞模式)
            // serverSocketChannel.socket().setSoTimeout(3000);
            // 启用或禁用SO_REUSEADDR选项.当设置为true时，允许在端口处于TIME_WAIT状态时重新绑定该端口。这在服务器重启或频繁绑定时
            // 非常有用。但是也可能造成上一个连接的脏数据包影响新建立的连接。
            // serverSocketChannel.socket().setReuseAddress(Boolean.TRUE);
            // 设置接收缓冲区大小。影响系统为接收数据分配的内存量
            //serverSocketChannel.socket().setReceiveBufferSize(BUF_SIZE);
            // 设置性能偏好权重，用于指导底层协议栈在连接建立时间，延迟和带宽之间做权衡。三个参数分别代表对"连接速度"、"低延迟"、"高带宽"的
            // 重视程度(数值越大越优先)。
            //serverSocketChannel.socket().setPerformancePreferences(1,1,1);
            //-------------------- 以上api已经基本不用，推荐使用下面的方式设置
//            serverSocketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
//                            .setOption(StandardSocketOptions.SO_REUSEADDR, true);

            serverSocketChannel.bind(new InetSocketAddress(PORT));
            serverSocketChannel.configureBlocking(false);
            AcceptThread acceptThread = new AcceptThread(serverSocketChannel,selectorThreads);
            // acceptThread是接收连接的线程，为啥不先启动。
            // 核心逻辑是生产者线程AcceptThread必须确保消费者线程SelectorThread已经准备好
            // 如果顺序反了，假如启动时系统负载很高，或者CPU调度导致SelectorThread启动稍微慢了一点。就在这几毫秒内，有客户端发起连接请求
            // AcceptThread 成功接收了请求要交给SelectorThread处理，但是此时SelectorThread还未起来，无法消费，
            // 会导致此次连接丢失或者失败
            // 必须现有"干活的人"(SelectorThread),在等着，才能派"拉客的人" 否则AcceptThread出去拉客，
            // 拉来了客人没人接待，会造成服务不可用
            // "先初始化消费者,后启动生产者"
            for(SelectorThread selectorThread : selectorThreads) {
                selectorThread.start();
            }
            acceptThread.start();

            try {
                Thread.sleep(Integer.MAX_VALUE);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
