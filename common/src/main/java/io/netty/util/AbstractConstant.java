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
package io.netty.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Base implementation of {@link Constant}.
 * 这个抽象类的泛型是要求是自己的子类，这个不会死循环吗，这个不会   这种被称为递归泛型 或自引用泛型
 * 它虽然在定义上看起来是自己引用自己 但没问题
 * 为什么不会死循环，要区分编译时类型检查   运行时内存分配
 * 当编译器看到AbstractConstant<T extends AbstractConstant<T>>时，它只是在建立一条规则：如果你要使用这个类，并且指定了泛型T，那么这个T
 * 必须是AbstractConstant的子类，编译器不会试图去无限展开这个定义类。
 * 泛型擦除， JVM运行这段代码时，它根本不知道T是什么，
 * 如果只是用AbstractConstant<T> implements  Constant<T> 那么你定义了一个doSomeThing方法 public T doSomeThing(){
 *     return (T)this;
 * } 如果不适用递归泛型，你只能返回AbstractConstant类型。你想继续链式调用，必须将返回值强转，很麻烦 且很不安全
 */
public abstract class AbstractConstant<T extends AbstractConstant<T>> implements Constant<T> {

    private static final AtomicLong uniqueIdGenerator = new AtomicLong();
    // 业务上的id,
    private final int id;
    private final String name;
    // 无业务逻辑 是个计算机看的，它的唯一目的是区分是对象a还是对象b，随机生成的，每次重启程序，同一个常量的这个值都会变
    // 上面的id是业务的，给人看的，比如你做一个http协议，get请求的id是1 post请求的id是2，如果你的代码里卖弄写if request.method.id==1
    // 这比if request.method.id == 65532 要有意义的多，即使程序重启，get的id依然是1，但是uniquifier可能会变成100
    // 还有如果你把这个对象保存到了数据库或者通过网络发送给了另一台服务器,如果只有uniquifier，服务器A创建了常量x uniquifier是55
    // 你把它发送给了服务器B，服务器B启动时，对应的对象uniquifier的值可能时60，无法对应。如果是用id，永远都是固定的，比如100
    // 还有可能你需要排序，根据业务id排序，而uniquifier是自动生成的，根据创建的先后顺序来的，无法业务排序。
    private final long uniquifier;

    /**
     * Creates a new instance.
     */
    protected AbstractConstant(int id, String name) {
        this.id = id;
        this.name = name;
        this.uniquifier = uniqueIdGenerator.getAndIncrement();
    }

    @Override
    public final String name() {
        return name;
    }

    @Override
    public final int id() {
        return id;
    }

    @Override
    public final String toString() {
        return name();
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public final int compareTo(T o) {
        if (this == o) {
            return 0;
        }

        @SuppressWarnings("UnnecessaryLocalVariable")
        AbstractConstant<T> other = o;
        int returnCode;

        returnCode = hashCode() - other.hashCode();
        if (returnCode != 0) {
            return returnCode;
        }

        if (uniquifier < other.uniquifier) {
            return -1;
        }
        if (uniquifier > other.uniquifier) {
            return 1;
        }

        throw new Error("failed to compare two different constants");
    }

}
