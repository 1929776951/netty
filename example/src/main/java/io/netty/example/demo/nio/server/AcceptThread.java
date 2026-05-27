package io.netty.example.demo.nio.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

public class AcceptThread extends Thread {

    private final Selector acceptSelector;
    private final ServerSocketChannel acceptServerSocket;
    private final SelectionKey acceptKey;

    private final Collection<SelectorThread> selectorThreads;
    private Iterator<SelectorThread> selectorIterator;

    public AcceptThread(ServerSocketChannel acceptServerSocket,Set<SelectorThread> selectorThreads)
            throws IOException {
        setDaemon(true);
        this.acceptSelector = Selector.open();
        this.acceptServerSocket = acceptServerSocket;
        //
        this.acceptKey = acceptServerSocket.register(acceptSelector, SelectionKey.OP_ACCEPT);
        this.selectorThreads = Collections.unmodifiableList(
                new ArrayList<SelectorThread>(selectorThreads));
        this.selectorIterator = this.selectorThreads.iterator();

    }

    @Override
    public void run() {
        try{
            while(acceptServerSocket.isOpen()){
                try{
                    accept();
                }catch (RuntimeException e){
                    System.err.println("Ignoring unexpected runtime exception");
                }catch (Exception e){
                    System.err.println("Ignoring unexpected exception");
                }

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                acceptSelector.close();
            } catch (IOException e) {
                System.out.println("ignored exception during selector close");
                e.printStackTrace();
            }
        }

    }

    private void accept() {
        try{
            acceptSelector.select();
            Iterator<SelectionKey> selectedKeys =
                    acceptSelector.selectedKeys().iterator();
            while(selectedKeys.hasNext()){
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();
                if (!key.isValid()) {
                    continue;
                }
                if (key.isAcceptable()) {
                    System.out.println("Acceptable事件触发===========================");
                    SocketChannel sc = null;
                    try{
                        // 服务端这里接收到一次accept事件后，可能不只是一个客户端连接事件，也就是一次accept事件不等于只有一个连接。
                        while(true){
                            sc = acceptServerSocket.accept();
                            if(sc == null){
                                break;
                            }
                            System.out.println("Accepted connection from " +sc.getRemoteAddress());
                            sc.configureBlocking(false);
                            if (!selectorIterator.hasNext()) {
                                selectorIterator = selectorThreads.iterator();
                            }
                            SelectorThread selectorThread = selectorIterator.next();
                            selectorThread.addAcceptedConnection(sc);
                        }
                    }catch (IOException e){
                        if(sc != null){
                            sc.close();
                        }
                        // 暂停10ms,取消OP_ACCEPT事件,select阻塞10ms后，又继续关注OP_ACCEPT
                        acceptKey.interestOps(0);// 不关注任何事件
                        try {
                            acceptSelector.select(10);
                        } catch (IOException e1) {
                            // ignore
                        } finally {
                            acceptKey.interestOps(SelectionKey.OP_ACCEPT);
                        }
                    }
                } else {
                    System.out.println("Unexpected ops in accept select ："+key.readyOps());
                }
            }

        }catch(Exception e){
            System.err.println("Ignoring IOException while selecting");
        }
    }




    /**
     * jdk7之前的把呢不能需要这么关闭  zookeeper源码里面的关闭方法
     * @param sc
     */
    @Deprecated
    private void fastCloseSock(SocketChannel sc) {
        if (sc != null) {
            try {
                // Hard close immediately, discarding buffers
                sc.socket().setSoLinger(true, 0);
            } catch (SocketException e) {
                System.out.println("Unable to set socket linger to 0, socket close"
                        + " may stall in CLOSE_WAIT");
                e.printStackTrace();
            }
            if (sc.isOpen() == false) {
                return;
            }
            try {
                /*
                 * The following sequence of code is stupid! You would think that
                 * only sock.close() is needed, but alas, it doesn't work that way.
                 * If you just do sock.close() there are cases where the socket
                 * doesn't actually close...
                 */
                sc.socket().shutdownOutput();
            } catch (IOException e) {
                // This is a relatively common exception that we can't avoid
                System.out.println("ignoring exception during output shutdown");
                e.printStackTrace();
            }
            try {
                sc.socket().shutdownInput();
            } catch (IOException e) {
                // This is a relatively common exception that we can't avoid
                System.out.println("ignoring exception during input shutdown");
                e.printStackTrace();
            }
            try {
                sc.socket().close();
            } catch (IOException e) {
                System.out.println("ignoring exception during socket close");
                e.printStackTrace();
            }
            try {
                sc.close();
            } catch (IOException e) {
                System.out.println("ignoring exception during socketchannel close");
                e.printStackTrace();
            }
        }
    }
}
