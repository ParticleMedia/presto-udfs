package com.facebook.presto.udfs;


import com.google.common.collect.ImmutableSet;
import com.facebook.presto.udfs.scala.hiveUdfs.NltkBaseTokenizeFunction;
import com.facebook.presto.spi.Plugin;

import java.util.Set;

/**
 * Created by ricky on 2023/7/10.
 */
public class FBUdfPlugin implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        /*
         * Presto 0.157 does not expose the interfaces to add SqlFunction objects directly
         * We can only add udfs via Annotations now
         *
         * Unsupported udfs right now:
         * Hash
         * Nvl
         */
        return ImmutableSet.<Class<?>>builder()
                .add(NltkBaseTokenizeFunction.class)
                .build();
    }
}