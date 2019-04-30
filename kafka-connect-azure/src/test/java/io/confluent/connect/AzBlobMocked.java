package io.confluent.connect;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import io.confluent.connect.Utils.AvroUtil;
import io.confluent.connect.Utils.ByteArrayUtil;
import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.Utils.JsonUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.azblob.storage.CompressionType;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class AzBlobMocked extends AzBlobSinkConnectorTestBase {
    private static final String BLOB_NAME = "blob";
    public File azMockDir;
    protected CloudBlobClient blobClient;

    @Rule
    public TemporaryFolder azMockRoot = new TemporaryFolder();

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "_");
        props.put(StorageCommonConfig.FILE_DELIM_CONFIG, "#");
        return props;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        azMockDir = azMockRoot.newFolder("AZ-tests-" + UUID.randomUUID().toString());

        CloudStorageAccount storageAccount = mockStorageAccount();
        blobClient = storageAccount.createCloudBlobClient();
        storage = new AzBlobStorage(connectorConfig, AZ_TEST_CONTAINER_NAME, storageAccount,
                blobClient, blobClient.getContainerReference(AZ_TEST_CONTAINER_NAME));
    }

    public CloudStorageAccount mockStorageAccount() throws Exception {
        CloudStorageAccount storageAccount = mock(CloudStorageAccount.class);
        CloudBlobClient blobClient = mock(CloudBlobClient.class);
        final CloudBlobContainer blobContainer = mock(CloudBlobContainer.class);
        CloudBlockBlob blockBlob = mock(CloudBlockBlob.class);
        BlobProperties blobProperties = mock(BlobProperties.class);

        when(storageAccount.createCloudBlobClient()).thenReturn(blobClient);
        when(blockBlob.exists()).thenReturn(true);
        when(blobContainer.exists()).thenReturn(true);
        when(blockBlob.getProperties()).thenReturn(blobProperties);
        when(blobProperties.getLength()).thenReturn(4096L);
        when(blobClient.getContainerReference(eq(AZ_TEST_CONTAINER_NAME))).thenReturn(blobContainer);
        when(blobContainer.getBlockBlobReference(eq(BLOB_NAME))).thenReturn(blockBlob);
        when(blobContainer.getBlockBlobReference(anyString())).thenReturn(blockBlob);
        when(blockBlob.getName()).thenReturn(BLOB_NAME);
        when(blockBlob.openInputStream()).thenReturn(mock(BlobInputStream.class));

        BlobOutputStream blobOutputStream = mock(BlobOutputStream.class);
        blobOutputStream = new BlobOutputStream() {
            OutputStream output;
            @Override
            public void write(byte[] bytes, int start, int end) throws IOException {
                File temp = null;
                try {
                    ArgumentCaptor<String> fileCaptor = ArgumentCaptor.forClass(String.class);
                    verify(blobContainer,atLeastOnce()).getBlockBlobReference(fileCaptor.capture());

                    String fileName = fileCaptor.getValue();
                    temp = new File(azMockDir.getPath()+"/"+fileName);
                    if(!temp.exists()) {
                        output = new FileOutputStream(temp.getPath()); }
                } catch (URISyntaxException | StorageException e) {
                    e.printStackTrace();
                }
                output.write(bytes, start, end);
            }

            @Override
            public void write(InputStream inputStream, long l) throws IOException, StorageException {
            }

            @Override
            public void flush() throws IOException {
                //output.flush();
            }

            @Override
            public void close() throws IOException {
                output.close();
            }
        };
        when(blockBlob.openOutputStream()).thenReturn(blobOutputStream);
        return storageAccount;
    }

    public Collection<Object> readRecords(String topicsDir, String directory, TopicPartition tp, long startOffset,
                                                 String extension, String zeroPadFormat) throws IOException {
        String fileKey = FileUtil.fileKeyToCommit(topicsDir, directory, tp, startOffset,
                extension, zeroPadFormat);
        CompressionType compressionType = CompressionType.NONE;
        if (extension.endsWith(".gz")) {
            compressionType = CompressionType.GZIP;
        }
        if (".avro".equals(extension)) {
            return readRecordsAvro(fileKey);
        } else if (extension.startsWith(".json")) {
            return readRecordsJson(fileKey, compressionType);
        } else if (extension.startsWith(".bin")) {
            return readRecordsByteArray(fileKey, compressionType,
                    AzBlobSinkConnectorConfig.FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT.getBytes());
        } else if (extension.startsWith(".customExtensionForTest")) {
            return readRecordsByteArray(fileKey, compressionType,
                    "SEPARATOR".getBytes());
        } else {
            throw new IllegalArgumentException("Unknown extension: " + extension);
        }
    }

    public Collection<Object> readRecordsAvro(String fileKey) throws IOException {
        InputStream in = new FileInputStream(azMockDir.getPath() + "/" + fileKey);
        return AvroUtil.getRecords(in);
    }

    public Collection<Object> readRecordsJson(String fileKey,
                                                     CompressionType compressionType) throws IOException {
        InputStream in = new FileInputStream(azMockDir.getPath() + "/" + fileKey);
        return JsonUtil.getRecords(compressionType.wrapForInput(in));
    }

    public Collection<Object> readRecordsByteArray(String fileKey, CompressionType compressionType,
                                                          byte[] lineSeparatorBytes) throws IOException {
        InputStream in = new FileInputStream(azMockDir.getPath() + "/" + fileKey);
        return ByteArrayUtil.getRecords(compressionType.wrapForInput(in), lineSeparatorBytes);
    }

    public List<String> listObjects(){
        File[] listOfFiles = azMockDir.listFiles();

        List<String> fileKeys = new ArrayList<>();
        if(listOfFiles.length > 0) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    fileKeys.add(file.getName());
                }
            }
        }
        return fileKeys;
    }
}
