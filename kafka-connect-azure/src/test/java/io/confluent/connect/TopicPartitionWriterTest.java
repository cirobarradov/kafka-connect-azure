package io.confluent.connect;

import io.confluent.common.utils.MockTime;
import io.confluent.connect.Utils.TimeUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.TopicPartitionWriter;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.apache.kafka.common.utils.Time.SYSTEM;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TopicPartitionWriterTest extends AzBlobMocked {

    // The default
    private static final String ZERO_PAD_FMT = "%010d";

    private RecordWriterProvider<AzBlobSinkConnectorConfig> writerProvider;
    private static String extension;
    private AzBlobStorage azBlobStorage;
    private AvroFormat format;

    Map<String, String> localProps = new HashMap<>();

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    public void setUp() throws Exception {
        super.setUp();
        AzBlobStorage storage = super.storage;
        format = new AvroFormat(storage);
        assertTrue(blobClient.getContainerReference(AZ_TEST_CONTAINER_NAME).exists());
        Format<AzBlobSinkConnectorConfig, String> format = new AvroFormat(storage);
        writerProvider = format.getRecordWriterProvider();
        extension = writerProvider.getExtension();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        localProps.clear();
    }

    private Struct createRecord(Schema schema, int ibase, float fbase) {
        return new Struct(schema)
                .put("boolean", true)
                .put("int", ibase)
                .put("long", (long) ibase)
                .put("float", fbase)
                .put("double", (double) fbase);
    }

    // Create a batch of records with incremental numeric field values. Total number of records is given by 'size'.
    private List<Struct> createRecordBatch(Schema schema, int size) {
        ArrayList<Struct> records = new ArrayList<>(size);
        int ibase = 16;
        float fbase = 12.2f;
        for (int i = 0; i < size; ++i) {
            records.add(createRecord(schema, ibase + i, fbase + i));
        }
        return records;
    }

    // Create a list of records by repeating the same record batch. Total number of records: 'batchesNum' x 'batchSize'
    private List<Struct> createRecordBatches(Schema schema, int batchSize, int batchesNum) {
        ArrayList<Struct> records = new ArrayList<>();
        for (int i = 0; i < batchesNum; ++i) {
            records.addAll(createRecordBatch(schema, batchSize));
        }
        return records;
    }

    // Given a list of records, create a list of sink records with contiguous offsets.
    private List<SinkRecord> createSinkRecords(List<Struct> records, String key, Schema schema) {
        return createSinkRecords(records, key, schema, 0);
    }

    // Given a list of records, create a list of sink records with contiguous offsets.
    private List<SinkRecord> createSinkRecords(List<Struct> records, String key, Schema schema, int startOffset) {
        ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 0; i < records.size(); ++i) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i),
                    i + startOffset));
        }
        return sinkRecords;
    }

    private void verify(List<String> expectedFileKeys, int expectedSize, Schema schema, List<Struct> records)
            throws IOException {
        List<String> actualFiles = listObjects();

        Collections.sort(actualFiles);
        Collections.sort(expectedFileKeys);
        assertThat(actualFiles, is(expectedFileKeys));

        int index = 0;
        for (String fileKey : actualFiles) {
            Collection<Object> actualRecords = readRecordsAvro(fileKey);
            assertEquals(expectedSize, actualRecords.size());
            for (Object avroRecord : actualRecords) {
                Object expectedRecord = format.getAvroData().fromConnectData(schema, records.get(index++));
                assertEquals(expectedRecord, avroRecord);
            }
        }
    }

    private String getTimebasedEncodedPartition(long timestamp) {
        long partitionDurationMs = (Long) parsedConfig.get(PartitionerConfig.PARTITION_DURATION_MS_CONFIG);
        String pathFormat = (String) parsedConfig.get(PartitionerConfig.PATH_FORMAT_CONFIG);
        String timeZone = (String) parsedConfig.get(PartitionerConfig.TIMEZONE_CONFIG);
        return TimeUtil.encodeTimestamp(partitionDurationMs, pathFormat, timeZone, timestamp);
    }

    public static class MockedWallclockTimestampExtractor implements TimestampExtractor {
        public final MockTime time;

        public MockedWallclockTimestampExtractor() {
            this.time = new MockTime();
        }

        @Override
        public void configure(Map<String, Object> config) {}

        @Override
        public Long extract(ConnectRecord<?> record) {
            return time.milliseconds();
        }
    }

    // Given a list of records, create a list of sink records with contiguous offsets.
    private List<SinkRecord> createSinkRecordsWithTimestamp(List<Struct> records, String key, Schema schema,
                                                            int startOffset, long startTime, long timeStep) {
        ArrayList<SinkRecord> sinkRecords = new ArrayList<>();
        for (int i = 0, offset = startOffset; i < records.size(); ++i, ++offset) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, records.get(i), offset,
                    startTime + offset * timeStep, TimestampType.CREATE_TIME));
        }
        return sinkRecords;
    }

    @Test
    public void testWriteRecordDefaultWithPaddingSeq() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
        setUp();
        Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context);
        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatch(schema, 9);
        Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);

        List<String> expectedFiles = new ArrayList<>();
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
        verify(expectedFiles, 3, schema, records);
    }

    @Test
    public void testWriteRecordDefaultWithPadding() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
        setUp();

        // Define the partitioner
        Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context);

        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);

        Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }
        // Test actual write
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
        List<String> expectedFiles = new ArrayList<>();
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, "%02d"));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, "%02d"));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, "%02d"));
        verify(expectedFiles, 3, schema, records);
    }

    @Test
    public void testWriteRecordTimeBasedPartitionWallclockRealtime() throws Exception {
        setUp();

        long timeBucketMs = TimeUnit.SECONDS.toMillis(5);
        long partitionDurationMs = TimeUnit.MINUTES.toMillis(1);

        // Define the partitioner
        Partitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
        parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, partitionDurationMs);
        parsedConfig.put(
                PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH_'min'=mm_'sec'=ss");
        partitioner.configure(parsedConfig);

        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context);

        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 6);
        Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 9), key, schema);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        long timestampFirst = SYSTEM.milliseconds();
        topicPartitionWriter.write();

        SYSTEM.sleep(timeBucketMs);

        sinkRecords = createSinkRecords(records.subList(9, 18), key, schema, 9);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        long timestampLater = SYSTEM.milliseconds();
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

        String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
        List<String> expectedFiles = new ArrayList<>();
        for (int i : new int[]{0, 3, 6}) {
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                    ZERO_PAD_FMT));
        }

        String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
        for (int i : new int[]{9, 12, 15}) {
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                    ZERO_PAD_FMT));
        }
        verify(expectedFiles, 3, schema, records);
    }

    @Test(expected = ConnectException.class)
    public void testWriteRecordTimeBasedPartitionWithNullTimestamp() throws Exception {
        localProps.put(
                AzBlobSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(1))
        );
        setUp();

        // Define the partitioner
        Partitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
        parsedConfig.put(PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, "Record");
        parsedConfig.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd");
        parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
        partitioner.configure(parsedConfig);

        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context);

        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 1, 1);
        // Just one bad record with null timestamp
        Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema, 0);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        topicPartitionWriter.write();
    }

    @Test
    public void testWriteRecordTimeBasedPartitionWallclockMockedWithScheduleRotation()
            throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG, "1000");
        localProps.put(
                AzBlobSinkConnectorConfig.ROTATE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.HOURS.toMillis(1))
        );
        localProps.put(
                AzBlobSinkConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS_CONFIG,
                String.valueOf(TimeUnit.MINUTES.toMillis(10))
        );
        setUp();

        // Define the partitioner
        TimeBasedPartitioner<FieldSchema> partitioner = new TimeBasedPartitioner<>();
        parsedConfig.put(PartitionerConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.DAYS.toMillis(1));
        parsedConfig.put(
                PartitionerConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockedWallclockTimestampExtractor.class.getName());
        partitioner.configure(parsedConfig);

        MockTime time = ((MockedWallclockTimestampExtractor) partitioner.getTimestampExtractor()).time;

        // Bring the clock to present.
        time.sleep(SYSTEM.milliseconds());
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context, time);

        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 6);
        Collection<SinkRecord> sinkRecords = createSinkRecords(records.subList(0, 3), key, schema);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        // No Records Written to Azure
        topicPartitionWriter.write();
        long timestampFirst = time.milliseconds();

        // 11 minutes
        time.sleep(TimeUnit.MINUTES.toMillis(11));
        // Records are written due to scheduled rotation
        topicPartitionWriter.write();

        sinkRecords = createSinkRecords(records.subList(3, 6), key, schema, 3);
        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        // More records later
        topicPartitionWriter.write();
        long timestampLater = time.milliseconds();

        // 11 minutes later, another scheduled rotation
        time.sleep(TimeUnit.MINUTES.toMillis(11));

        // Again the records are written due to scheduled rotation
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String encodedPartitionFirst = getTimebasedEncodedPartition(timestampFirst);
        String encodedPartitionLater = getTimebasedEncodedPartition(timestampLater);

        String dirPrefixFirst = partitioner.generatePartitionedPath(TOPIC, encodedPartitionFirst);
        List<String> expectedFiles = new ArrayList<>();
        for (int i : new int[]{0}) {
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefixFirst, TOPIC_PARTITION, i, extension,
                    ZERO_PAD_FMT));
        }

        String dirPrefixLater = partitioner.generatePartitionedPath(TOPIC, encodedPartitionLater);
        for (int i : new int[]{3}) {
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefixLater, TOPIC_PARTITION, i, extension,
                    ZERO_PAD_FMT));
        }
        verify(expectedFiles, 3, schema, records);
    }

    @Test
    public void testWriteRecordDefaultWithEmptyTopicsDir() throws Exception {
        localProps.put(StorageCommonConfig.TOPICS_DIR_CONFIG, "");
        setUp();

        // Define the partitioner
        Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(
                TOPIC_PARTITION, writerProvider, partitioner,  connectorConfig, context);

        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatches(schema, 3, 3);

        Collection<SinkRecord> sinkRecords = createSinkRecords(records, key, schema);

        for (SinkRecord record : sinkRecords) {
            topicPartitionWriter.buffer(record);
        }

        // Test actual write
        topicPartitionWriter.write();
        topicPartitionWriter.close();

        String dirPrefix = partitioner.generatePartitionedPath(TOPIC, "partition=" + PARTITION);
        List<String> expectedFiles = new ArrayList<>();
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 3, extension, ZERO_PAD_FMT));
        expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, dirPrefix, TOPIC_PARTITION, 6, extension, ZERO_PAD_FMT));
        verify(expectedFiles, 3, schema, records);
    }


}