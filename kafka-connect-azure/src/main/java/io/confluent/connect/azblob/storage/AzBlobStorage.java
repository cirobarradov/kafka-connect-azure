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

package io.confluent.connect.azblob.storage;

import java.io.OutputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.avro.file.SeekableInput;

import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;

import io.confluent.connect.storage.Storage;
import io.confluent.connect.storage.common.util.StringUtils;

import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;

import static io.confluent.connect.storage.common.util.StringUtils.isNotBlank;

public class AzBlobStorage implements Storage<AzBlobSinkConnectorConfig, Iterable<ListBlobItem>> {

  private final String containerName;
  private final AzBlobSinkConnectorConfig conf;
  private static final String VERSION_FORMAT = "APN/1.0 Confluent/1.0 KafkaAZBlobConnector/%s";
  private final CloudStorageAccount storageAccount;
  private final CloudBlobClient blobClient;
  private final CloudBlobContainer container;

  /**
   * Construct an AzBlobStorage class given a configuration and an AZ Storage account + container.
   *
   * @param conf the AzBlobStorage configuration.
   */
  public AzBlobStorage(AzBlobSinkConnectorConfig conf, String url) throws URISyntaxException,
          StorageException, InvalidKeyException {
    this.conf = conf;
    this.containerName = conf.getContainerName();

    // Retrieve storage account from connection-string.
    storageAccount = CloudStorageAccount.parse(conf.getStorageConnectionString());

    // Create the blob client.
    blobClient = storageAccount.createCloudBlobClient();

    // Get a reference to a container.
    // The container name must be lower case.
    container = blobClient.getContainerReference(conf.getContainerName());

    // Create the container if it does not exist.
    container.createIfNotExists();
  }

  // Visible for testing.
  public AzBlobStorage(AzBlobSinkConnectorConfig conf, String containerName,
                       CloudStorageAccount storageAccount, CloudBlobClient blobClient,
                       CloudBlobContainer container) {
    this.conf = conf;
    this.containerName = containerName;
    this.storageAccount = storageAccount;
    this.blobClient = blobClient;
    this.container = container;
  }

  @Override
  public boolean exists(String name) {
    try {
      return isNotBlank(name) && container.getBlockBlobReference(name).exists();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public OutputStream append(String filename) {
    throw new UnsupportedOperationException(
            "Append operation in Azure blob storage is currently not supported");
  }

  @Override
  public void delete(String name) {
    throw new UnsupportedOperationException(
            "Deletion of blobs in Azure blob storage is currently not supported");
  }

  @Override
  public void close() {
  }

  @Override
  public String url() {
    return container.getUri().toString();
  }

  @Override
  public Iterable<ListBlobItem> list(String path) {
    return container.listBlobs(path);
  }

  @Override
  public AzBlobSinkConnectorConfig conf() {
    return conf;
  }

  @Override
  public SeekableInput open(String path, AzBlobSinkConnectorConfig conf) {
    throw new UnsupportedOperationException(
            "File reading is currently not supported in Azure Blob Storage Connector");
  }

  @Override
  public boolean create(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OutputStream create(String path, AzBlobSinkConnectorConfig conf, boolean overwrite) {
    return create(path, overwrite);
  }

  public BlobOutputStream create(String path, boolean overwrite) {
    if (!overwrite) {
      throw new UnsupportedOperationException(
              "Creating a file without overwriting is currently not supported"
              + "in Azure Blob Storage Connector");
    }

    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException("Path can not be empty!");
    }

    CloudBlockBlob blob = null;
    BlobOutputStream stream;
    try {
      blob = container.getBlockBlobReference(path);
      stream = blob.openOutputStream();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    return stream;
  }

  public boolean containerExists() throws URISyntaxException, StorageException {
    return isNotBlank(containerName) && blobClient.getContainerReference(containerName).exists();
  }

}
