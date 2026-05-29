/*
 * Copyright 2025 The Netty Project
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
package io.netty.util.concurrent;

import java.util.concurrent.Executor;

/**
 * Executor that is aware its execution thread.
 */
// 让执行器具备"感知自身工作线程"的能力。方便在代码中判断当前线程是否属于该执行器。
// 在netty等异步框架中，经常需要判断 当前代码是否运行在指定执行器的工作线程上？某个线程是否由该执行器管理？
public interface ThreadAwareExecutor extends Executor {
    /**
     * Return {@code true} if the given {@link Thread} is used by this {@link ThreadAwareExecutor} to execute
     * work.
     */
    boolean isExecutorThread(Thread thread);
}
