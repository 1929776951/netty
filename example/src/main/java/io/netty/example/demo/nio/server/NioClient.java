package io.netty.example.demo.nio.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;


public class NioClient {

    private static final int SERVER_PORT = 8866;

    private static final String SERVER_HOST = "localhost";



    public static void main(String[] args) throws IOException, InterruptedException {
            for(int i = 0;i< 10;i++){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            Selector selector = Selector.open();
                            SocketChannel sc = SocketChannel.open();
                            sc.configureBlocking(false);
                            sc.register(selector, SelectionKey.OP_CONNECT);
                            sc.connect(new InetSocketAddress(SERVER_HOST, SERVER_PORT));
                            while (true) {
                                selector.select();
                                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                                Iterator<SelectionKey> iterator = selectedKeys.iterator();
                                while (iterator.hasNext()) {
                                    SelectionKey key = iterator.next();
                                    iterator.remove();
                                    if (key.isConnectable()) {
                                        SocketChannel socketChannel = (SocketChannel) key.channel();

                                        if (socketChannel.finishConnect()) {
                                            System.out.println("连接服务端成功,客户端使用的端口:" + socketChannel.getLocalAddress().toString());
                                            key.interestOps(SelectionKey.OP_READ);
                                        }

                                    }
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).start();
            }




    }
}
