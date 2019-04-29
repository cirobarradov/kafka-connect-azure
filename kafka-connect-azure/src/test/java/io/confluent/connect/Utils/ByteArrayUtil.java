package io.confluent.connect.Utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class ByteArrayUtil {
    public static Collection<Object> getRecords(InputStream in, byte[] lineSeparatorBytes) throws IOException {
        byte[] bytes = IOUtils.toByteArray(in);
        return splitLines(lineSeparatorBytes, bytes);
    }

    private static boolean isMatch(byte[] lineSeparatorBytes, byte[] input, int pos) {
        for (int i = 0; i < lineSeparatorBytes.length; i++) {
            if (lineSeparatorBytes[i] != input[pos+i]) {
                return false;
            }
        }
        return true;
    }

    private static Collection<Object> splitLines(byte[] lineSeparatorBytes, byte[] input) {
        List<Object> records = new ArrayList<>();
        int lineStart = 0;
        for (int i = 0; i < input.length; i++) {
            if (isMatch(lineSeparatorBytes, input, i)) {
                records.add(Arrays.copyOfRange(input, lineStart, i));
                lineStart = i + lineSeparatorBytes.length;
                i = lineStart;
            }
        }
        if (lineStart != input.length) {
            records.add(Arrays.copyOfRange(input, lineStart, input.length));
        }
        return records;
    }
}
