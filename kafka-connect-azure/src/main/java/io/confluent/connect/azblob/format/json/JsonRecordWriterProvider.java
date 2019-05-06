/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.connect.azblob.format.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.storage.CompressionType;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonRecordWriterProvider implements RecordWriterProvider<AzBlobSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String EXTENSION = ".json";
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes();
  private final AzBlobStorage storage;
  private final ObjectMapper mapper;
  private final JsonConverter converter;

  JsonRecordWriterProvider(AzBlobStorage storage, JsonConverter converter) {
    this.storage = storage;
    this.mapper = new ObjectMapper();
    this.converter = converter;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final AzBlobSinkConnectorConfig conf, final String filename) {
    final boolean gz = conf.getString(AzBlobSinkConnectorConfig.COMPRESSION_TYPE_CONFIG)
            .equals("gzip");
    try {
      return new RecordWriter() {
        final BlobOutputStream azBlobOutputStream = gz
                ? storage.create(filename + ".gz", true)
                : storage.create(filename,true);
        final OutputStream azBlobOutputWrapper = gz
                ? CompressionType.GZIP.wrapForOutput(azBlobOutputStream) : azBlobOutputStream;
        final JsonGenerator writer = mapper.getFactory()
                .createGenerator(azBlobOutputWrapper)
                .setRootValueSeparator(null);

        @Override
        public void write(SinkRecord record) {
          log.trace("Sink record: {}", record);
          try {
            Object value = record.value();
            if (value instanceof Struct) {
              byte[] rawJson = converter
                      .fromConnectData(record.topic(), record.valueSchema(), value);
              azBlobOutputWrapper.write(rawJson);
              azBlobOutputWrapper.write(LINE_SEPARATOR_BYTES);
            } else {
              writer.writeObject(value);
              writer.writeRaw(LINE_SEPARATOR);
            }
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void commit() {
          try {
            // Flush is required here, because closing the writer will close the underlying AZ
            // output stream before committing any data to AZ.
            writer.flush();
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void close() {
          try {
            writer.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      };
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
