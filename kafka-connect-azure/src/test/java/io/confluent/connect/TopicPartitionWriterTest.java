package io.confluent.connect;

import io.confluent.connect.AzBlobMocked;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.TopicPartitionWriter;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertTrue;

public class TopicPartitionWriterTest extends AzBlobMocked {

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

//  private void verify(List<String> expectedFileKeys, int expectedSize, Schema schema, List<Struct> records)
//          throws IOException {
//    List<String> actualFiles = new ArrayList<>();
//  }

    @Test
    public void testWriteRecordDefaultWithPaddingSeq() throws Exception {
        localProps.put(AzBlobSinkConnectorConfig.FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, "2");
        setUp();
        Partitioner<FieldSchema> partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);
        TopicPartitionWriter topicPartitionWriter = new TopicPartitionWriter(TOPIC_PARTITION, writerProvider, partitioner, connectorConfig, context);
        String key = "key";
        Schema schema = createSchema();
        List<Struct> records = createRecordBatch(schema, 10);
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
//    verify(expectedFiles, 3, schema, records);
    }

}