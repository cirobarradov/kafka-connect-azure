package io.confluent.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.format.json.JsonFormat;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.storage.CompressionType;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkTask.class,AzBlobStorage.class})
public class DataWriterJsonTest extends AzBlobMocked {

  JsonFormat format;
  Partitioner<FieldSchema> partitioner;
  protected static final String ZERO_PAD_FMT = "%010d";
  protected final ObjectMapper mapper = new ObjectMapper();
  protected final String extension = ".json";
  private JsonConverter converter;
  protected AzBlobSinkTask task;

  protected Map<String, String> localProps = new HashMap<>();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.putAll(localProps);
    return props;
  }

  public void setUp() throws Exception {
    super.setUp();
    converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

    partitioner = new DefaultPartitioner<>();
    partitioner.configure(parsedConfig);
    format = new JsonFormat(storage);
    PowerMockito.whenNew(AzBlobStorage.class).withAnyArguments().thenReturn(storage);
    assertTrue(blobClient.getContainerReference(AZ_TEST_CONTAINER_NAME).exists());
  }

  @Test
  public void testNoSchema() throws Exception {
    localProps.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    setUp();
    task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

    List<SinkRecord> sinkRecords = createJsonRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), ".json");
  }

  @Test
  public void testWithSchema() throws Exception {
    localProps.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG, JsonFormat.class.getName());
    setUp();

    task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 0, context.assignment());
    task.initialize(context);
    task.start(properties);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets, context.assignment(), ".json");
  }

  protected List<SinkRecord> createJsonRecordsWithoutSchema(int size, long startOffset, Set<TopicPartition> partitions) {
    String key = "key";
    int ibase = 12;

    List<SinkRecord> sinkRecords = new ArrayList<>();
    for (long offset = startOffset, total = 0; total < size; ++offset) {
      for (TopicPartition tp : partitions) {
        String record = "{\"schema\":{\"type\":\"struct\",\"fields\":[ " +
                "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"}," +
                "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"}," +
                "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"}," +
                "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}]," +
                "\"payload\":" +
                "{\"booleanField\":\"true\"," +
                "\"intField\":" + String.valueOf(ibase) + "," +
                "\"longField\":" + String.valueOf((long) ibase) + "," +
                "\"stringField\":str" + String.valueOf(ibase) +
                "}}";
        sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, key, null, record, offset));
        if (++total >= size) {
          break;
        }
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

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        String extension)
          throws IOException {
    verify(sinkRecords, validOffsets, partitions, extension, false);
  }

  protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                        String extension, boolean skipFileListing)
          throws IOException {
    if (!skipFileListing) {
      verifyFileListing(validOffsets, partitions, extension);
    }

    for (TopicPartition tp : partitions) {
      for (int i = 1, j = 0; i < validOffsets.length; ++i) {
        long startOffset = validOffsets[i - 1];
        long size = validOffsets[i] - startOffset;

        FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, extension, ZERO_PAD_FMT);
        Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                extension, ZERO_PAD_FMT);
        // assertEquals(size, records.size());
        verifyContents(sinkRecords, j, records);
        j += size;
      }
    }
  }

  protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions,
                                   String extension) throws IOException {
    List<String> expectedFiles = new ArrayList<>();
    for (TopicPartition tp : partitions) {
      expectedFiles.addAll(getExpectedFiles(validOffsets, tp, extension));
    }
    verifyFileListing(expectedFiles);
  }

  protected void verifyFileListing(List<String> expectedFiles) throws IOException {
    List<String> summary = listObjects();
    List<String> actualFiles = new ArrayList<>();
    ListIterator<String> iterator = summary.listIterator();
    while(iterator.hasNext()) {
      actualFiles.add(iterator.next());
    }
  }
  protected List<String> getExpectedFiles (long[] validOffsets, TopicPartition tp, String extension) {
    List<String> expectedFiles = new ArrayList<>();
    for (int i = 1; i < validOffsets.length; ++i) {
      long startOffset = validOffsets[i - 1];
      expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
              extension, ZERO_PAD_FMT));
    }
    return expectedFiles;
  }

  protected String getDirectory(String topic, int partition) {
    String encodedPartition = "partition=" + String.valueOf(partition);
    return partitioner.generatePartitionedPath(topic, encodedPartition);
  }

  protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records)
          throws IOException{
    for (Object jsonRecord : records) {
      SinkRecord expectedRecord = expectedRecords.get(startIndex++);
      Object expectedValue = expectedRecord.value();
      if (expectedValue instanceof Struct) {
        byte[] expectedBytes = converter.fromConnectData(TOPIC, expectedRecord.valueSchema(), expectedRecord.value());
        expectedValue = mapper.readValue(expectedBytes, Object.class);
      }
      assertEquals(expectedValue, jsonRecord);
    }
  }

}