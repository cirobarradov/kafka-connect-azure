package io.confluent.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.format.json.JsonFormat;
import io.confluent.connect.azblob.storage.AzBlobStorage;
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

import java.util.*;

import static junit.framework.TestCase.assertTrue;

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
  public void testWriteRecord() throws Exception {
    localProps.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG,JsonFormat.class.getName());
    localProps.put(AzBlobSinkConnectorConfig.FLUSH_SIZE_CONFIG,"9");
    setUp();
    AzBlobSinkTask task = new AzBlobSinkTask();
    task.initialize(context);
    task.start(properties);

    List<SinkRecord> sinkRecords = createRecordsInterleaved(7 * context.assignment().size(), 0, context.assignment());
    task.put(sinkRecords);

    task.close(context.assignment());
    task.stop();
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

}