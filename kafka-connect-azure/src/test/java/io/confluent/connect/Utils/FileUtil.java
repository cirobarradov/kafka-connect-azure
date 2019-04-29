package io.confluent.connect.Utils;

import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.common.TopicPartition;

public class FileUtil {
    public static final String TEST_FILE_DELIM = "#";
    public static final String TEST_DIRECTORY_DELIM = "_";

    public static String fileKey(String topicsPrefix, String keyPrefix, String name) {
        String suffix = keyPrefix + TEST_DIRECTORY_DELIM + name;
        return StringUtils.isNotBlank(topicsPrefix)
                ? topicsPrefix + TEST_DIRECTORY_DELIM + suffix
                : suffix;
    }

    public static String fileKeyToCommit(String topicsPrefix, String dirPrefix, TopicPartition tp, long startOffset,
                                         String extension, String zeroPadFormat) {
        String name = tp.topic()
                + TEST_FILE_DELIM
                + tp.partition()
                + TEST_FILE_DELIM
                + String.format(zeroPadFormat, startOffset)
                + extension;
        return fileKey(topicsPrefix, dirPrefix, name);
    }

}
