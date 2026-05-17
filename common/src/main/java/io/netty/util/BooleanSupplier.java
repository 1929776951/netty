/*
 * Copyright 2016 The Netty Project
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

/**
 * Represents a supplier of {@code boolean}-valued results.
 */
public interface BooleanSupplier {
    /**
     * Gets a boolean value.
     * @return a boolean value.
     * @throws Exception If an exception occurs.
     */
    boolean get() throws Exception;

    /**
     * A supplier which always returns {@code false} and never throws.
     */
    //这种“返回固定值的 Supplier”设计模式，在 Java 类库（尤其是像 Netty 这种高性能框架）中非常常见，通常被称为 Flyweight（享元）模式
    // 的一种应用。
    //避免对象频繁创建（性能优化） someMethod(() -> true);虽然 Java 的 Lambda 在某些情况下会进行优化（如 invokedynamic 缓存），
    // 但在泛型推断、序列化或特定 JVM 实现下，频繁地传递 Lambda 表达式或者匿名内部类，有可能会产生大量的临时对象，增加 GC（垃圾回收）压力。
    //解决泛型与 API 的兼容性问题
    // public void retryOperation(BooleanSupplier condition) { ... } 假设这是一个通用的重试机制
    // 笨办法： 你没法直接传 true 进去，因为类型不匹配（boolean 不是 BooleanSupplier）。你被迫每次都要写 () -> true。
    //优雅办法： 直接传 BooleanSupplier.TRUE_SUPPLIER。
    BooleanSupplier FALSE_SUPPLIER = new BooleanSupplier() {
        @Override
        public boolean get() {
            return false;
        }
    };

    /**
     * A supplier which always returns {@code true} and never throws.
     */
    BooleanSupplier TRUE_SUPPLIER = new BooleanSupplier() {
        @Override
        public boolean get() {
            return true;
        }
    };
}
