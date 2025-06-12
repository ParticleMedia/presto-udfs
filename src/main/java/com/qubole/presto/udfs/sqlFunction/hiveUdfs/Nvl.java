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

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;

/**
 * NVL 函数实现 - 如果第一个参数为 null，则返回第二个参数值，否则返回第一个参数值
 */
@ScalarFunction("nvl")
@Description("返回第二个参数如果第一个为null，否则返回第一个参数")
public final class Nvl
{
    private Nvl() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BIGINT)
    public static Long nvlBigint(
            @SqlType(StandardTypes.BIGINT) Long value,
            @SqlType(StandardTypes.BIGINT) Long defaultValue)
    {
        return value == null ? defaultValue : value;
    }

    @TypeParameter("E")
    @SqlType(StandardTypes.DOUBLE)
    public static Double nvlDouble(
            @SqlType(StandardTypes.DOUBLE) Double value,
            @SqlType(StandardTypes.DOUBLE) Double defaultValue)
    {
        return value == null ? defaultValue : value;
    }

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean nvlBoolean(
            @SqlType(StandardTypes.BOOLEAN) Boolean value,
            @SqlType(StandardTypes.BOOLEAN) Boolean defaultValue)
    {
        return value == null ? defaultValue : value;
    }

    @TypeParameter("E")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice nvlVarchar(
            @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.VARCHAR) Slice defaultValue)
    {
        return value == null ? defaultValue : value;
    }

    @TypeParameter("E")
    @SqlType("E")
    public static Object nvlGeneric(
            @SqlType("E") Object value,
            @SqlType("E") Object defaultValue)
    {
        return value == null ? defaultValue : value;
    }
}