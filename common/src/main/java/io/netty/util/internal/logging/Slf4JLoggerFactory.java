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
package io.netty.util.internal.logging;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

/**
 * Logger factory which creates a <a href="https://www.slf4j.org/">SLF4J</a>
 * logger.
 */
public class Slf4JLoggerFactory extends InternalLoggerFactory {

    @SuppressWarnings("deprecation")
    public static final InternalLoggerFactory INSTANCE = new Slf4JLoggerFactory();

    /**
     * @deprecated Use {@link #INSTANCE} instead.
     */
    @Deprecated
    public Slf4JLoggerFactory() {
    }

    Slf4JLoggerFactory(boolean failIfNOP) {
        assert failIfNOP; // Should be always called with true.
        if (LoggerFactory.getILoggerFactory() instanceof NOPLoggerFactory) {
            throw new NoClassDefFoundError("NOPLoggerFactory not supported");
        }
    }

    @Override
    public InternalLogger newInstance(String name) {
        return wrapLogger(LoggerFactory.getLogger(name));
    }

    // package-private for testing.
    static InternalLogger wrapLogger(Logger logger) {
        return logger instanceof LocationAwareLogger ?
                new LocationAwareSlf4JLogger((LocationAwareLogger) logger) : new Slf4JLogger(logger);
    }

    static InternalLoggerFactory getInstanceWithNopCheck() {
        return NopInstanceHolder.INSTANCE_WITH_NOP_CHECK;
    }

    // 为什么不直接定义一个镜头变量 还有高一个静态内部类
    // 这是一种初始化延迟持有者模式 主要是为了懒加载和性能优化
    // 如果直接定义一个静态变量，不用这个静态私有内部类包装，那么当JVM加载外部类Slf4JLoggerFactory的时候，就会初始化，如果初始化过程
    // 过程中new Slf4JLoggerFactory(true) 这个操作很重，哪怕你不需要用到这个变量，这部分开销也会被强制执行，从而拖慢了主类的加载速度
    // 用静态内部类后，加载Slf4JLoggerFactory这个类的时候，NopInstanceHolder不会被加载，只有等到getInstanceWithNopCheck()方法
    // 被调用的时候，才会加载。 如果运行期间完全没有用到这个方法，那这部分开销就完全省下来了
    private static final class NopInstanceHolder {
        private static final InternalLoggerFactory INSTANCE_WITH_NOP_CHECK = new Slf4JLoggerFactory(true);
    }
}
