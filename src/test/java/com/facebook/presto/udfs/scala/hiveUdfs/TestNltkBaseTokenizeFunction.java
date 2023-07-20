package com.facebook.presto.udfs.scala.hiveUdfs;

import com.facebook.presto.common.block.Block;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;
//import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static com.facebook.presto.udfs.scala.hiveUdfs.NltkBaseTokenizeFunction.NltkBaseTokenize;
import static org.junit.Assert.assertEquals;

/**
 * If not able to run locally, try adding this to java run env: --add-opens java.base/java.lang=ALL-UNNAMED
 */
public class TestNltkBaseTokenizeFunction {

    @Test
    public void NltkBaseTokenizeTest() {
        String input = "hello so than world I think that's good";
        Slice inputSlice = Slices.utf8Slice(input);

        String[] expected = {"hello", "world", "think", "that's", "good"};

        Block block = NltkBaseTokenize(inputSlice);
        int count = block.getPositionCount();

        assertEquals(5, count);
        for(int i = 0; i < count; i++) {
            int length = block.getSliceLength(i);
            Slice currentSlice = block.getSlice(i, 0, length);
            String candidate = new String(currentSlice.getBytes(), StandardCharsets.UTF_8);
            assertEquals(expected[i], candidate);
        }
    }
}