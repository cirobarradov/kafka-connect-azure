package io.confluent.connect;

import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.storage.AzBlobStorage;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkTask.class,AzBlobStorage.class})
public class AzBlobSinkTaskTest extends AzBlobMocked {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        PowerMockito.whenNew(AzBlobStorage.class).withAnyArguments().thenReturn(storage);
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

    @Test
    public void testTaskType() throws Exception {
        AzBlobSinkTask task = new AzBlobSinkTask();
        SinkTask.class.isAssignableFrom(task.getClass());
    }

    @Test
    public void mockingAzureServices() throws Exception{
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);
        List <SinkRecord> sinkRecordList = createRecords(5,0,
                Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
        task.put(sinkRecordList);
        task.close(context.assignment());
        task.stop();
    }


}
