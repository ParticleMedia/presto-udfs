package com.facebook.presto.udfs.scala.hiveUdfs;

import io.airlift.slice.Slice;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class NltkBaseTokenizeFunction
{
//    private static final int INITIAL_LENGTH = 128;

    private static final String[] STOP_WORDS = {
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
            "you", "you're", "you've", "you'll", "you'd", "your", "yours", "yourself",
            "yourselves", "he", "him", "his", "himself", "she", "she's", "her", "hers",
            "herself", "it", "it's", "its", "itself", "they", "them", "their", "theirs",
            "themselves", "what", "which", "who", "whom", "this", "that", "that'll", "these",
            "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
            "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because",
            "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
            "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out",
            "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where",
            "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no",
            "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just",
            "don", "don't", "should", "should've", "now", "d", "ll", "m", "o", "re", "ve", "y", "ain", "aren",
            "aren't", "couldn", "couldn't", "didn", "didn't", "doesn", "doesn't", "hadn", "hadn't", "hasn",
            "hasn't", "haven", "haven't", "isn", "isn't", "ma", "mightn", "mightn't", "mustn", "mustn't",
            "needn", "needn't", "shan", "shan't", "shouldn", "shouldn't", "wasn", "wasn't", "weren",
            "weren't", "won", "won't", "wouldn", "wouldn't"};

    private static final String[] STRING_PUNCTUATION = {
            "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/", ":", ";", "<", "=", 
            ">", "?", "@", "[", "\\", "]", "^", "_", "`", "{", "|", "}", "~"
    };

    private static final Set<String> STOP_WORDS_SET = new HashSet<>(Arrays.asList(STOP_WORDS));
    private static final Set<String> STRING_PUCNTUATION_SET = new HashSet<>(Arrays.asList(STRING_PUNCTUATION));

    public NltkBaseTokenizeFunction() {}

    @SqlType("array(varchar)")
    @ScalarFunction(value = "NltkBaseTokenize")
    @Description("Remove stop words and tokenize input string.")
    public static Block NltkBaseTokenize(@SqlType(StandardTypes.VARCHAR) Slice string) {
        String delimeterStr = " ";
        Slice delimiter = Slices.utf8Slice(delimeterStr);

        return splitWithStopWords(string, delimiter, string.length() + 1);
    }


    public static Block splitWithStopWords(@SqlType("varchar(x)") Slice string, @SqlType("varchar(y)") Slice delimiter, @SqlType(StandardTypes.BIGINT) long limit)
    {
//        checkCondition(limit > 0, INVALID_FUNCTION_ARGUMENT, "Limit must be positive");
//        checkCondition(limit <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "Limit is too large");
        BlockBuilder parts = VARCHAR.createBlockBuilder(null, 1, string.length());
        // If limit is one, the last and only element is the complete string
        if (limit == 1) {
            VARCHAR.writeSlice(parts, string);
            return parts.build();
        }

        int index = 0;
        while (index < string.length()) {
            int splitIndex = string.indexOf(delimiter, index);
            // Found split?
            if (splitIndex < 0) {
                break;
            }
            if (delimiter.length() == 0) {
                // For zero-length delimiter, create 1-length splits.
                splitIndex++;
            }

            String candidate = new String(string.getBytes(index, splitIndex - index), StandardCharsets.UTF_8);

            if (!STOP_WORDS_SET.contains(candidate.toLowerCase()) && !STRING_PUCNTUATION_SET.contains(candidate)) {
                // Add the part from current index to found split
                VARCHAR.writeSlice(parts, string, index, splitIndex - index);
            }

            // Continue searching after delimiter
            index = splitIndex + delimiter.length();
            // Reached limit-1 parts so we can stop
            if (parts.getPositionCount() == limit - 1) {
                break;
            }
        }

        String candidate = new String(string.getBytes(index, string.length() - index), StandardCharsets.UTF_8);
        if (!STOP_WORDS_SET.contains(candidate.toLowerCase()) && !STRING_PUCNTUATION_SET.contains(candidate)) {
            // Rest of string
            VARCHAR.writeSlice(parts, string, index, string.length() - index);
        }

        // Since parquet does not support empty arrays
        if (parts.getPositionCount() == 0) {
            VARCHAR.writeString(parts, "");
        }

        return parts.build();
    }
//
//    public static void main(String[] args) {
//        String input = "hello so than world I think that's good";
//        Slice inputSlice = Slices.utf8Slice(input);
//
//        Block block = NltkBaseTokenize(inputSlice);
//        int count = block.getPositionCount();
//        System.out.println("Total number of slices:" + count);
//        for(int i = 0; i < count; i++) {
//            int length = block.getSliceLength(i);
//            Slice currentSlice = block.getSlice(i, 0, length);
//            String candidate = new String(currentSlice.getBytes(), StandardCharsets.UTF_8);
//            System.out.println("Getting candidate:" + candidate);
//        }
//    }
}