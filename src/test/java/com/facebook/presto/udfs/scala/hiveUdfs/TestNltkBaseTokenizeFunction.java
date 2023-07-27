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

    private void DoTest(String input, String[] expected) {
        Slice inputSlice = Slices.utf8Slice(input);
        Block block = NltkBaseTokenize(inputSlice);
        int count = block.getPositionCount();

        assertEquals(expected.length, count);
        for(int i = 0; i < count; i++) {
            int length = block.getSliceLength(i);
            Slice currentSlice = block.getSlice(i, 0, length);
            String candidate = new String(currentSlice.getBytes(), StandardCharsets.UTF_8);
            assertEquals(expected[i], candidate);
        }
    }

    @Test
    public void NltkBaseTokenizeTest() {
        this.DoTest(
            "5.5 Magnitude Earthquake Strikes Northern California , No Tsunami Threat",
            new String[] {"5.5","Magnitude","Earthquake","Strikes","Northern","California","Tsunami","Threat"}
        );
    }

    @Test
    public void NltkBaseTokenizeTest2() {
        this.DoTest(
            "A ` base camp ' has popped up on vacant land in the Florida Keys . What 's happening there ?",
            new String[] {"base","camp","popped","vacant","land","Florida","Keys","'s","happening"}
        );
    }

    @Test
    public void NltkBaseTokenizeTest3() {
        this.DoTest(
            "Scott County Inmate Roster -- 3-11-23",
            new String[] {"Scott", "County", "Inmate", "Roster", "--", "3-11-23"}
        );
    }

    @Test
    public void NltkBaseTokenizeTest4() {
        this.DoTest(
            "The",
            new String[] {""}
        );
    }

    @Test
    public void NltkBaseTokenizeTest5() {
        this.DoTest(
            "The the .",
            new String[] {""}
        );
    }

    @Test
    public void NltkBaseTokenizeTest6() {
        this.DoTest(
            "aaa",
            new String[] {"aaa"}
        );
    }

    @Test
    public void NltkBaseTokenizeTest7() {
        this.DoTest(
            "",
            new String[] {""}
        );
    }

}