/*
 * Copyright 2013-2016 Qubole
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qubole.presto.udfs.sqlFunction.hiveUdfs;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.util.Objects;

@ScalarFunction("hash")
@Description("计算任意类型变量的哈希值")
public final class Hash
{
    private Hash() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BIGINT)
    public static long hash(@SqlType("E") Object value, @TypeParameter("E") Type type)
    {
        if (value == null) {
            return 0;
        }

        Block block;
        if (value instanceof Block) {
            block = (Block) value;
        }
        else {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            appendToBlock(type, value, blockBuilder);
            block = blockBuilder.build();
        }

        if (block.getPositionCount() == 0) {
            return 0;
        }

        return computeHash(type, block);
    }

    private static long computeHash(Type type, Block block)
    {
        long hash = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                // 根据不同类型计算哈希值
                Object element = readNativeValue(type, block, position);
                if (element != null) {
                    hash = 31 * hash + element.hashCode();
                }
            }
        }
        return hash;
    }

    // 从Block中读取原生Java值
    private static Object readNativeValue(Type type, Block block, int position)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) {
            return type.getLong(block, position);
        }
        else if (javaType == double.class) {
            double value = type.getDouble(block, position);
            if (Double.isNaN(value)) {
                throw new IllegalArgumentException("Invalid argument to hash(): NaN");
            }
            return value;
        }
        else if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        else if (javaType == Slice.class) {
            return type.getSlice(block, position);
        }
        else {
            return type.getObject(block, position);
        }
    }

    private static void appendToBlock(Type type, Object value, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();

        if (javaType == long.class) {
            type.writeLong(blockBuilder, (Long) value);
        }
        else if (javaType == double.class) {
            if (Double.isNaN((Double) value)) {
                throw new IllegalArgumentException("Invalid argument to hash(): NaN");
            }
            type.writeDouble(blockBuilder, (Double) value);
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == Slice.class) {
            type.writeSlice(blockBuilder, (Slice) value);
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }
}