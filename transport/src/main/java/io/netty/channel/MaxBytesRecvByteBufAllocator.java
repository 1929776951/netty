/*
 * Copyright 2015 The Netty Project
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

import java.util.Map.Entry;

/**
 * {@link RecvByteBufAllocator} that limits a read operation based upon a maximum value per individual read
 * and a maximum amount when a read operation is attempted by the event loop.
 */
// 单独基于字节数 来限制读取存在明显的缺陷,而基于消息数更合理
// 粘包与拆包问题  TCP是流式协议，假设你设置最大读取64KB，这64KB可能不是一个完整的消息 不便于处理，还要等待下一次读取才能获取完整消息
// 如果限制字节数，如果客户端发送的都是极小的包,那么为了达到64KB，可能Netty需要调用底层read几千次,导致用户态和内核态频繁切换,CPU消耗巨大
// 目前此类已经处于"被废弃"的状态
public interface MaxBytesRecvByteBufAllocator extends RecvByteBufAllocator {
    /**
     * Returns the maximum number of bytes to read per read loop.
     * a {@link ChannelInboundHandler#channelRead(ChannelHandlerContext, Object) channelRead()} event.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     */
    int maxBytesPerRead();

    /**
     * Sets the maximum number of bytes to read per read loop.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     */
    MaxBytesRecvByteBufAllocator maxBytesPerRead(int maxBytesPerRead);

    /**
     * Returns the maximum number of bytes to read per individual read operation.
     * a {@link ChannelInboundHandler#channelRead(ChannelHandlerContext, Object) channelRead()} event.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     */
    int maxBytesPerIndividualRead();

    /**
     * Sets the maximum number of bytes to read per individual read operation.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     */
    MaxBytesRecvByteBufAllocator maxBytesPerIndividualRead(int maxBytesPerIndividualRead);

    /**
     * Atomic way to get the maximum number of bytes to read for a read loop and per individual read operation.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     * @return The Key is from {@link #maxBytesPerRead()}. The Value is from {@link #maxBytesPerIndividualRead()}
     */
    Entry<Integer, Integer> maxBytesPerReadPair();

    /**
     * Sets the maximum number of bytes to read for a read loop and per individual read operation.
     * If this value is greater than 1, an event loop might attempt to read multiple times to procure bytes.
     * @param maxBytesPerRead see {@link #maxBytesPerRead(int)}
     * @param maxBytesPerIndividualRead see {@link #maxBytesPerIndividualRead(int)}
     */
    MaxBytesRecvByteBufAllocator maxBytesPerReadPair(int maxBytesPerRead, int maxBytesPerIndividualRead);
}
