/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect;

import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.azblob.format.json.JsonFormat;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class AzBlobSinkConnectorConfigTest extends AzBlobSinkConnectorTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testStorageClass() {
    // No real test case yet
    connectorConfig = new AzBlobSinkConnectorConfig(properties);
    assertEquals(
            AzBlobStorage.class,
            connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG)
    );
  }

  @Test
  public void testUndefinedURL() {
    properties.remove(StorageCommonConfig.STORE_URL_CONFIG);
    connectorConfig = new AzBlobSinkConnectorConfig(properties);
    assertNull(connectorConfig.getString(StorageCommonConfig.STORE_URL_CONFIG));
  }

  @Test
  public void testRecommendedValues() {
    List<Object> expectedStorageClasses = Arrays.<Object>asList(AzBlobStorage.class);
    List<Object> expectedFormatClasses = Arrays.<Object>asList(
            AvroFormat.class,
            JsonFormat.class
    );
    List<Object> expectedPartitionerClasses = Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
    );

    List<ConfigValue> values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if (val.value() instanceof Class) {
        switch (val.name()) {
          case StorageCommonConfig.STORAGE_CLASS_CONFIG:
            assertEquals(expectedStorageClasses, val.recommendedValues());
            break;
          case AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG:
            assertEquals(expectedFormatClasses, val.recommendedValues());
            break;
          case PartitionerConfig.PARTITIONER_CLASS_CONFIG:
            assertEquals(expectedPartitionerClasses, val.recommendedValues());
            break;
        }
      }
    }
  }

  @Test
  public void testAvroDataConfigSupported() {
    properties.put(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, "true");
    properties.put(AvroDataConfig.CONNECT_META_DATA_CONFIG, "false");
    connectorConfig = new AzBlobSinkConnectorConfig(properties);
    assertEquals(true, connectorConfig.get(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG));
    assertEquals(false, connectorConfig.get(AvroDataConfig.CONNECT_META_DATA_CONFIG));
  }

  @Test
  public void testVisibilityForPartitionerClassDependentConfigs() {
    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
    List<ConfigValue> values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    assertDefaultPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, FieldPartitioner.class.getName());
    assertFieldPartitionerVisibility();

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, DailyPartitioner.class.getName());
    values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, HourlyPartitioner.class.getName());
    values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    assertTimeBasedPartitionerVisibility(values);

    properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            TimeBasedPartitioner.class.getName()
    );
    values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);

    Partitioner<?> klass = new Partitioner<FieldSchema>() {
      @Override
      public void configure(Map<String, Object> config) {}

      @Override
      public String encodePartition(SinkRecord sinkRecord) {
        return null;
      }

      @Override
      public String generatePartitionedPath(String topic, String encodedPartition) {
        return null;
      }

      @Override
      public List<FieldSchema> partitionFields() {
        return null;
      }
    };

    properties.put(
            PartitionerConfig.PARTITIONER_CLASS_CONFIG,
            klass.getClass().getName()
    );
    values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    assertNullPartitionerVisibility(values);
  }

  @Test
  public void testConfigurableCredentialProvider() {
    final String CONNECTION_STRING = "Sample_Connection_String";

    properties.put(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING, CONNECTION_STRING);

    List<ConfigValue> values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      if(val.name().equals(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING)){
        assertEquals(CONNECTION_STRING, val.value());
      }
    }
  }

  private void assertDefaultPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertFieldPartitionerVisibility() {
    List<ConfigValue> values;
    values = AzBlobSinkConnectorConfig.getConfig().validate(properties);
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
          assertTrue(val.visible());
          break;
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertFalse(val.visible());
          break;
      }
    }
  }

  private void assertTimeBasedPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_FIELD_NAME_CONFIG:
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
          assertFalse(val.visible());
          break;
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }

  private void assertNullPartitionerVisibility(List<ConfigValue> values) {
    for (ConfigValue val : values) {
      switch (val.name()) {
        case PartitionerConfig.PARTITION_DURATION_MS_CONFIG:
        case PartitionerConfig.PATH_FORMAT_CONFIG:
        case PartitionerConfig.LOCALE_CONFIG:
        case PartitionerConfig.TIMEZONE_CONFIG:
          assertTrue(val.visible());
          break;
      }
    }
  }
}
