package com.facebook.presto.udfs.aggr;

import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.block.BlockBuilder;

import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction("avg_double")
public class AverageAggregation
{
    @InputFunction
    public static void input(
            AggrLongAndDoubleState state,
            @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @CombineFunction
    public static void combine(
            AggrLongAndDoubleState state,
            AggrLongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(AggrLongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            DOUBLE.writeDouble(out, value / count);
        }
    }
}