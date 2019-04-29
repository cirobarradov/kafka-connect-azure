package io.confluent.connect;

import io.confluent.connect.Utils.AvroUtil;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static junit.framework.TestCase.assertTrue;


@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkTask.class,AzBlobStorage.class})
public class AzBlobSinkTaskTest extends DataWriterAvroTest {

    protected Map<String, String> localProps = new HashMap<>();

    public void setUp() throws Exception {
        super.setUp();
        PowerMockito.whenNew(AzBlobStorage.class).withAnyArguments().thenReturn(storage);
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        java.util.List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
            }
        }
        return sinkRecords;
    }

    public static class CustomPartitioner<T> extends DefaultPartitioner<T> {
        @Override
        public void configure(Map<String, Object> map) {
            assertTrue("Custom parameters were not passed down to the partitioner implementation",
                    map.containsKey("custom.partitioner.config"));
        }
    }

    @Test
    public void testTaskType() throws Exception {
        AzBlobSinkTask task = new AzBlobSinkTask();
        SinkTask.class.isAssignableFrom(task.getClass());
    }

    @Test
    public void testWriteRecord() throws Exception{
        setUp();
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);

        List <SinkRecord> sinkRecordList = createRecords(7,0,
                Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
        task.put(sinkRecordList);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecordList, validOffsets);
    }

    @Test
    public void testRecoveryWithPartialFile() throws Exception {
        setUp();

        // Upload partial file.
        List<SinkRecord> sinkRecords = createRecords(2, 0,
                Collections.singleton(new TopicPartition(TOPIC,PARTITION)));
        byte[] partialData = AvroUtil.putRecords(sinkRecords, format.getAvroData());
        String fileKey = FileUtil.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION,
                0, extension, ZERO_PAD_FMT);

        // Accumulate rest of the records.
        sinkRecords.addAll(createRecords(5, 2,
                Collections.singleton(new TopicPartition(TOPIC,PARTITION))));

        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsSpanningMultipleParts() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        setUp();

        List<SinkRecord> sinkRecords = createRecords(1100, 0,
                Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);

        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 1000};
        verify(sinkRecords, validOffsets);
    }

    protected List<SinkRecord> createRecordsWithPrimitive(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = Schema.INT32_SCHEMA;
        int record = 12;

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
            }
        }
        return sinkRecords;
    }


    @Test
    public void testWriteRecordWithPrimitives() throws Exception {
        setUp();

        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);

        List<SinkRecord> sinkRecords = createRecordsWithPrimitive(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testPartitionerConfig() throws Exception {
        localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        localProps.put("custom.partitioner.config", "arbitrary value");
        setUp();
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);
    }


}
