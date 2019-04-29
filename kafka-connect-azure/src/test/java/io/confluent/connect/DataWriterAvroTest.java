package io.confluent.connect;

import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaProjector;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DataWriterAvroTest extends AzBlobMocked {
    AvroFormat format;
    Partitioner<FieldSchema> partitioner;
    private static final String ZERO_PAD_FMT = "%010d";
    private final String extension = ".avro";

    public void setUp() throws Exception {
        super.setUp();
        partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        format = new AvroFormat(storage);
    }

    protected String getDirectory() {
        return getDirectory(TOPIC, PARTITION);
    }

    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets) throws IOException {
        verify(sinkRecords, validOffsets, Collections.singleton(new TopicPartition(TOPIC, PARTITION)), false);
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions)
            throws IOException {
        verify(sinkRecords, validOffsets, partitions, false);
    }

    protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records) {
        Schema expectedSchema = null;
        for (Object avroRecord : records) {
            if (expectedSchema == null) {
                expectedSchema = expectedRecords.get(startIndex).valueSchema();
            }
            Object expectedValue = SchemaProjector.project(expectedRecords.get(startIndex).valueSchema(),
                    expectedRecords.get(startIndex++).value(),
                    expectedSchema);
            Object value = format.getAvroData().fromConnectData(expectedSchema, expectedValue);
            // AvroData wraps primitive types so their schema can be included. We need to unwrap
            // NonRecordContainers to just their value to properly handle these types
            if (value instanceof NonRecordContainer) {
                value = ((NonRecordContainer) value).getValue();
            }
            if (avroRecord instanceof Utf8) {
                assertEquals(value, avroRecord.toString());
            } else {
                assertEquals(value, avroRecord);
            }
        }
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                          boolean skipFileListing)
            throws IOException {
        for (TopicPartition tp : partitions) {
            for (int i = 1, j = 0; i < validOffsets.length; ++i) {
                long startOffset = validOffsets[i - 1];
                long size = validOffsets[i] - startOffset;

                FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, extension, ZERO_PAD_FMT);
                Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                        extension, ZERO_PAD_FMT);
                assertEquals(size, records.size());
                verifyContents(sinkRecords, j, records);
                j += size;
            }
        }
    }

}
