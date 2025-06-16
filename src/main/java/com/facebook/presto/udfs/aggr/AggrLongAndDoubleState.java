package com.facebook.presto.udfs.aggr;

import io.trino.spi.function.AccumulatorState;

public interface AggrLongAndDoubleState
        extends AccumulatorState
{
    long getLong();

    void setLong(long value);

    double getDouble();

    void setDouble(double value);
}