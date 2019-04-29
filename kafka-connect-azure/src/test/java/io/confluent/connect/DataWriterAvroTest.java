package io.confluent.connect;

import io.confluent.common.utils.Time;
import io.confluent.connect.Utils.AvroUtil;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static io.confluent.connect.avro.AvroData.AVRO_TYPE_ENUM;
import static io.confluent.connect.avro.AvroData.CONNECT_ENUM_DOC_PROP;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class DataWriterAvroTest extends AzBlobMocked {
    AvroFormat format;
    Partitioner<FieldSchema> partitioner;
    protected static final String ZERO_PAD_FMT = "%010d";
    protected final String extension = ".avro";
    protected AzBlobSinkTask task;

    Map<String, String> localProps = new HashMap<>();

    public void setUp() throws Exception {
        super.setUp();
        partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        format = new AvroFormat(storage);
    }

    @Test
    public void testWriteRecords() throws Exception {
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecords(7);
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);

    }

    @Test
    public void testWriteRecordsOfEnumsWithEnhancedAvroData() throws Exception {
        localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
        localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createRecordsWithEnums(7, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsOfUnionsWithEnhancedAvroData() throws Exception {
        localProps.put(StorageSinkConnectorConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
        localProps.put(StorageSinkConnectorConfig.CONNECT_META_DATA_CONFIG, "true");
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createRecordsWithUnion(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));

        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6, 9, 12, 15, 18, 21, 24, 27};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testRecoveryWithPartialFile() throws Exception {
        setUp();

        // Upload partial file.
        List<SinkRecord> sinkRecords = createRecords(2);
        byte[] partialData = AvroUtil.putRecords(sinkRecords, format.getAvroData());
        String fileKey = FileUtil.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, extension, ZERO_PAD_FMT);

        // Accumulate rest of the records.
        sinkRecords.addAll(createRecords(5, 2));

        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test
    public void testWriteRecordsInMultiplePartitions() throws Exception {
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecords(7, 0, context.assignment());
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testWriteInterleavedRecordsInMultiplePartitions() throws Exception {
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 0, context.assignment());
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testWriteInterleavedRecordsInMultiplePartitionsNonZeroInitialOffset() throws Exception {
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 9, context.assignment());
        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {9, 12, 15};
        verify(sinkRecords, validOffsets, context.assignment());
    }

    @Test
    public void testPreCommitOnSchemaIncompatibilityRotation() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
        setUp();

        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(2, 0);

        // Perform write
        task.put(sinkRecords);

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = task.preCommit(null);

        long[] validOffsets = {1, -1};

        verifyOffsets(offsetsToCommit, validOffsets, context.assignment());

        task.close(context.assignment());
        task.stop();
    }

    @Test
    public void testProjectNone() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
        setUp();

        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createRecordsWithAlteringSchemas(7, 0);

        // Perform write
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 1, 2, 3, 4, 5, 6};
        verify(sinkRecords, validOffsets);
    }

    @Test(expected= ConnectException.class)
    public void testProjectNoVersion() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG, "2");
        localProps.put(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG, "BACKWARD");
        setUp();

        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createRecordsNoVersion(1, 0);
        sinkRecords.addAll(createRecordsWithAlteringSchemas(7, 0));

        // Perform write
        try {
            task.put(sinkRecords);
        } finally {
            task.close(context.assignment());
            task.stop();
            long[] validOffsets = {};
            verify(Collections.<SinkRecord>emptyList(), validOffsets);
        }
    }



    protected String getDirectory() {
        return getDirectory(TOPIC, PARTITION);
    }

    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    /**
     * Return a list of new records starting at zero offset.
     *
     * @param size the number of records to return.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecords(int size) {
        return createRecords(size, 0);
    }

    /**
     * Return a list of new records starting at the given offset.
     *
     * @param size the number of records to return.
     * @param startOffset the starting offset.
     * @return the list of records.
     */
    protected List<SinkRecord> createRecords(int size, long startOffset) {
        return createRecords(size, startOffset, Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    }

    protected List<SinkRecord> createRecords(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
            }
        }
        return sinkRecords;
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

    protected List<SinkRecord> createRecordsWithEnums(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createEnumSchema();
        SchemaAndValue valueAndSchema = new SchemaAndValue(schema, "bar");
        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema.value(), offset));
            }
        }
        return sinkRecords;
    }

    public Schema createEnumSchema() {
        // Enums are just converted to strings, original enum is preserved in parameters
        SchemaBuilder builder = SchemaBuilder.string().name("TestEnum");
        builder.parameter(CONNECT_ENUM_DOC_PROP, null);
        builder.parameter(AVRO_TYPE_ENUM, "TestEnum");
        for(String enumSymbol : new String[]{"foo", "bar", "baz"}) {
            builder.parameter(AVRO_TYPE_ENUM+"."+enumSymbol, enumSymbol);
        }
        return builder.build();
    }

    protected List<SinkRecord> createRecordsWithUnion(
            int size,
            long startOffset,
            Set<TopicPartition> partitions
    ) {
        Schema recordSchema1 = SchemaBuilder.struct().name("Test1")
                .field("test", Schema.INT32_SCHEMA).optional().build();
        Schema recordSchema2 = SchemaBuilder.struct().name("io.confluent.Test2")
                .field("test", Schema.INT32_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct()
                .name("io.confluent.connect.avro.Union")
                .field("int", Schema.OPTIONAL_INT32_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Test1", recordSchema1)
                .field("io.confluent.Test2", recordSchema2)
                .build();

        SchemaAndValue valueAndSchemaInt = new SchemaAndValue(schema, new Struct(schema).put("int", 12));
        SchemaAndValue valueAndSchemaString = new SchemaAndValue(schema, new Struct(schema).put("string", "teststring"));

        Struct schema1Test = new Struct(schema).put("Test1", new Struct(recordSchema1).put("test", 12));
        SchemaAndValue valueAndSchema1 = new SchemaAndValue(schema, schema1Test);

        Struct schema2Test = new Struct(schema).put("io.confluent.Test2", new Struct(recordSchema2).put("test", 12));
        SchemaAndValue valueAndSchema2 = new SchemaAndValue(schema, schema2Test);

        String key = "key";
        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + 4 * size;) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaInt.value(), offset++));
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchemaString.value(), offset++));
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema1.value(), offset++));
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, valueAndSchema2.value(), offset++));
            }
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createRecordsInterleaved(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset, total = 0; total < size; ++offset) {
            for (TopicPartition tp : partitions) {
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), Schema.STRING_SCHEMA, key, schema, record, offset));
                if (++total >= size) {
                    break;
                }
            }
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createRecordsNoVersion(int size, long startOffset) {
        String key = "key";
        Schema schemaNoVersion = SchemaBuilder.struct().name("record")
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .build();

        Struct recordNoVersion = new Struct(schemaNoVersion);
        recordNoVersion.put("boolean", true)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset; offset < startOffset + size; ++offset) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schemaNoVersion,
                    recordNoVersion, offset));
        }
        return sinkRecords;
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

    protected void verifyOffsets(Map<TopicPartition, OffsetAndMetadata> actualOffsets, long[] validOffsets,
                                 Set<TopicPartition> partitions) {
        int i = 0;
        Map<TopicPartition, OffsetAndMetadata> expectedOffsets = new HashMap<>();
        for (TopicPartition tp : partitions) {
            long offset = validOffsets[i++];
            if (offset >= 0) {
                expectedOffsets.put(tp, new OffsetAndMetadata(offset, ""));
            }
        }
        assertTrue(Objects.equals(actualOffsets, expectedOffsets));
    }

    protected List<SinkRecord> createRecordsWithAlteringSchemas(int size, long startOffset) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);
        Schema newSchema = createNewSchema();
        Struct newRecord = createNewRecord(newSchema);

        int limit = (size / 2) * 2;
        boolean remainder = size % 2 > 0;
        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset; offset < startOffset + limit; ++offset) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record, offset));
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, newSchema, newRecord, ++offset));
        }
        if (remainder) {
            sinkRecords.add(new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, key, schema, record,
                    startOffset + size - 1));
        }
        return sinkRecords;
    }

    protected List<SinkRecord> createRecordsWithTimestamp(
            int size,
            long startOffset,
            Set<TopicPartition> partitions,
            Time time
    ) {
        String key = "key";
        Schema schema = createSchema();
        Struct record = createRecord(schema);

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            for (long offset = startOffset; offset < startOffset + size; ++offset) {
                sinkRecords.add(new SinkRecord(
                        TOPIC,
                        tp.partition(),
                        Schema.STRING_SCHEMA,
                        key,
                        schema,
                        record,
                        offset,
                        time.milliseconds(),
                        TimestampType.CREATE_TIME
                ));
            }
        }
        return sinkRecords;
    }

    protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp) {
        List<String> expectedFiles = new ArrayList<>();
        for (int i = 1; i < validOffsets.length; ++i) {
            long startOffset = validOffsets[i - 1];
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                    extension, ZERO_PAD_FMT));
        }
        return expectedFiles;
    }

    protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions) throws IOException {
        List<String> expectedFiles = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            expectedFiles.addAll(getExpectedFiles(validOffsets, tp));
        }
        verifyFileListing(expectedFiles);
    }

    protected void verifyFileListing(List<String> expectedFiles) throws IOException {
        List<String> summaries = listObjects();
        List<String> actualFiles = new ArrayList<>();
        for (String summary : summaries) {
            actualFiles.add(summary);
        }

        Collections.sort(actualFiles);
        Collections.sort(expectedFiles);
        assertThat(actualFiles, is(expectedFiles));
    }

}
