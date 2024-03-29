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

import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.format.avro.AvroFormat;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.storage.StorageSinkTestBase;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.hive.HiveConfig;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.schema.SchemaCompatibility;
import io.confluent.connect.storage.schema.StorageSchemaCompatibility;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AzBlobSinkConnectorTestBase extends StorageSinkTestBase {

  private static final Logger log = LoggerFactory.getLogger(AzBlobSinkConnectorTestBase.class);

  protected static final String AZ_TEST_URL = "az.url";
  protected static final String AZ_TEST_CONTAINER_NAME = "kafka.container";
  protected static final String AZ_TEST_CONNECTION_STRING = "az.container";
  protected static final Time SYSTEM_TIME = new SystemTime();

  protected AzBlobSinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected Map<String, Object> parsedConfig;
  protected SchemaCompatibility compatibility;
  protected AzBlobStorage storage;

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      log.info(
              "Starting test: {}.{}",
              description.getTestClass().getSimpleName(),
              description.getMethodName()
      );
    }
  };

  @Override
  protected Map<String, String> createProps() {
    url = AZ_TEST_URL;
    Map<String, String> props = super.createProps();
    props.put(StorageCommonConfig.STORAGE_CLASS_CONFIG, "io.confluent.connect.azblob.storage.AzBlobStorage");
    props.put(AzBlobSinkConnectorConfig.AZ_STORAGE_CONTAINER_NAME, AZ_TEST_CONTAINER_NAME);
    props.put(AzBlobSinkConnectorConfig.AZ_STORAGEACCOUNT_CONNECTION_STRING, AZ_TEST_CONNECTION_STRING);
    props.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG, AvroFormat.class.getName());
    props.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, PartitionerConfig.PARTITIONER_CLASS_DEFAULT.getName());
    props.put(PartitionerConfig.PARTITION_FIELD_NAME_CONFIG, "int");
    props.put(PartitionerConfig.PATH_FORMAT_CONFIG, "'year'=YYYY_'month'=MM_'day'=dd_'hour'=HH");
    props.put(PartitionerConfig.LOCALE_CONFIG, "en");
    props.put(PartitionerConfig.TIMEZONE_CONFIG, "America/Los_Angeles");
    props.put(HiveConfig.HIVE_CONF_DIR_CONFIG, "America/Los_Angeles");
    return props;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    connectorConfig = Mockito.spy(new AzBlobSinkConnectorConfig(properties));
    topicsDir = connectorConfig.getString(StorageCommonConfig.TOPICS_DIR_CONFIG);
    parsedConfig = new HashMap<>(connectorConfig.plainValues());
    compatibility = StorageSchemaCompatibility.getCompatibility(
            connectorConfig.getString(HiveConfig.SCHEMA_COMPATIBILITY_CONFIG));
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
}

