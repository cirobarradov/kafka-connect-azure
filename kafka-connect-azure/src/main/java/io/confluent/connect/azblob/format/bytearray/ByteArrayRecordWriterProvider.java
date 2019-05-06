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

package io.confluent.connect.azblob.format.bytearray;

import com.microsoft.azure.storage.blob.BlobOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.storage.CompressionType;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteArrayRecordWriterProvider implements
        RecordWriterProvider<AzBlobSinkConnectorConfig> {

  private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
  private final AzBlobStorage storage;
  private final ByteArrayConverter converter;
  private final String extension;
  private final byte[] lineSeparatorBytes;

  ByteArrayRecordWriterProvider(AzBlobStorage storage, ByteArrayConverter converter) {
    this.storage = storage;
    this.converter = converter;
    this.extension = storage.conf().getByteArrayExtension();
    this.lineSeparatorBytes = storage.conf().getFormatByteArrayLineSeparator().getBytes();
  }

  @Override
  public String getExtension() {
    return extension;
  }

  @Override
  public RecordWriter getRecordWriter(final AzBlobSinkConnectorConfig conf, final String filename) {
    final boolean gz = conf.getString(AzBlobSinkConnectorConfig.COMPRESSION_TYPE_CONFIG)
            .equals("gzip");
    return new RecordWriter() {
      BlobOutputStream azBlobOutputStream = gz ? storage.create(filename + ".gz",
              true) : storage.create(filename,true);
      final OutputStream azBlobOutputWrapper = gz
              ? CompressionType.GZIP.wrapForOutput(azBlobOutputStream) : azBlobOutputStream;

      @Override
      public void write(SinkRecord record) {
        log.trace("Sink record: {}", record);
        try {
          byte[] bytes = converter.fromConnectData(
                  record.topic(), record.valueSchema(), record.value());
          azBlobOutputWrapper.write(bytes);
          azBlobOutputWrapper.write(lineSeparatorBytes);
        } catch (IOException | DataException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          azBlobOutputWrapper.flush();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          azBlobOutputWrapper.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }
}
