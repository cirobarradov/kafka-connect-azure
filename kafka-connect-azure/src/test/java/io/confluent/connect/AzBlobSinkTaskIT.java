package io.confluent.connect;

import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AzBlobSinkTaskIT extends StorageSinkTestBase {

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("short", Schema.OPTIONAL_INT16_SCHEMA)
            .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
            .field("long", Schema.OPTIONAL_INT64_SCHEMA)
            .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    private HashMap<String, String> connProps;
    private SinkTaskContext sinkTaskContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        connProps = new HashMap<String, String>();
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING, "DefaultEndpointsProtocol=" +
                "https;AccountName=hashedin;AccountKey=/s6JZV6nMN7P1z9bSEbKCDHJ2tAH4Kcn8OY9thJ5HgQlBskIg2h4eMgY81bRH" +
                "Lq3dTg2V11zc/heh1x3129YHw==;EndpointSuffix=core.windows.net");
        connProps.put(AzBlobSinkConnectorConfig.AZ_STORAGE_CONTAINER_NAME, "azblob2");
        connProps.put("format.class", "io.confluent.connect.azblob.format.bytearray.ByteArrayFormat");
        connProps.put("storage.class", "io.confluent.connect.azblob.storage.AzBlobStorage");
//        connProps.put("schema.generator.class", "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator");
        connProps.put("flush.size", "2");
        //connProps.put("value.convertor","org.apache.kafka.connect.converters.ByteArrayConverter");
        //connProps.put("azBlob.compression.type","gzip");

        connProps.put(StorageCommonConfig.STORE_URL_CONFIG, "someurl");

    }

    @After
    public void tearDown() throws IOException {
    }

    @Test
    public void put() throws Exception {
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        System.out.print(connProps);
        task.start(connProps);

        final Struct struct = new Struct(SCHEMA)
                .put("firstName", "Alex")
                .put("lastName", "Smith")
                .put("bool", true)
                .put("short", (short) 1234)
                .put("byte", (byte) -32)
                .put("long", 12425436L)
                .put("float", (float) 2356.3)
                .put("double", -2436546.56457)
                .put("age", 21);

        final String topic = "test-topic";

        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 0));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 1));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 2));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 3));
        records.add(new SinkRecord(topic, 12, null, null, SCHEMA, struct, 4));

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0, total = 0; total < 3; ++offset) {
            for (TopicPartition tp : context.assignment()) {
                byte[] record = ("{\"schema\":{\"type\":\"struct\",\"fields\":[ " +
                        "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"}," +
                        "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"}," +
                        "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"}," +
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}]," +
                        "\"payload\":" +
                        "{\"booleanField\":\"true\"," +
                        "\"intField\":" + String.valueOf(12) + "," +
                        "\"longField\":" + String.valueOf((long) 12) + "," +
                        "\"stringField\":str" + String.valueOf(12) +
                        "}}").getBytes();
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, "key", null, record, offset));
                if (++total >= 3) {
                    break;
                }
            }
        }
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();
    }

}