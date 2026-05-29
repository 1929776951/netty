/*
 * Copyright 2024 The Netty Project
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
package io.netty.channel;

import io.netty.util.concurrent.Future;

/**
 * {@link EventLoopGroup} for {@link IoEventLoop}s.
 */
// 专门设计这个IoEventLoopGroup 是为了职责分离   EventLoopGroup是通用接口
    // 当前类是专用接口，只针对I/O事件场景
    // Netty支持多种底层I/O模型 NIO Epoll(Linux专属) KQueue(BSD/macOS专属) IO_Uring(Linux5.1+最新异步I/O)
    // 此接口作为顶层抽象，让不同I/O模式的实现类可以统一对外暴露，同时通过isCompatible和isIoType来约束各种的适用范围
public interface IoEventLoopGroup extends EventLoopGroup {

    @Override
    IoEventLoop next();

    /**
     * @deprecated Use {@link #register(IoHandle)}
     */
    @Deprecated
    @Override
    default ChannelFuture register(Channel channel) {
        return next().register(channel);
    }

    /**
     * @deprecated Use {@link #register(IoHandle)}
     */
    @Deprecated
    @Override
   default ChannelFuture register(ChannelPromise promise) {
        return next().register(promise);
    }

    /**
     * Register the {@link IoHandle} to the {@link EventLoop} for I/O processing.
     *
     * @param handle        the {@link IoHandle} to register.
     * @return              the {@link Future} that is notified once the operations completes.
     */
    default Future<IoRegistration> register(IoHandle handle) {
        return next().register(handle);
    }

    /**
     * Returns {@code true} if the given type is compatible with this {@link IoEventLoopGroup} and so can be registered
     * to the contained {@link IoEventLoop}s, {@code false} otherwise.
     *
     * @param handleType    the type of the {@link IoHandle}.
     * @return              if compatible of not.
     */
    default boolean isCompatible(Class<? extends IoHandle> handleType) {
        return next().isCompatible(handleType);
    }

    /**
     * Returns {@code true} if the given {@link IoHandler} type is used by this {@link IoEventLoopGroup},
     * {@code false} otherwise.
     *
     * @param handlerType the type of the {@link IoHandler}.
     * @return            if used or not.
     */
    default boolean isIoType(Class<? extends IoHandler> handlerType) {
        return next().isIoType(handlerType);
    }
}
