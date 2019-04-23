package io.confluent.connect;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AzBlobMocked extends AzBlobSinkConnectorTestBase {
    private static final String blobName = "blob";
    protected String port;

    @Rule
    public TemporaryFolder azMockRoot = new TemporaryFolder();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        CloudStorageAccount storageAccount = mockStorageAccount();
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        storage = new AzBlobStorage(connectorConfig, AZ_TEST_CONTAINER_NAME, storageAccount,
                blobClient, blobClient.getContainerReference(AZ_TEST_CONTAINER_NAME));

        port = url.substring(url.lastIndexOf(":") + 1);
        File azMockDir = azMockRoot.newFolder("AZ-tests-" + UUID.randomUUID().toString());
        System.out.println("Create folder: " + azMockDir.getCanonicalPath());
    }



    public static CloudStorageAccount mockStorageAccount() throws Exception {
        CloudStorageAccount storageAccount = mock(CloudStorageAccount.class);
        CloudBlobClient blobClient = mock(CloudBlobClient.class);
        CloudBlobContainer blobContainer = mock(CloudBlobContainer.class);
        CloudBlockBlob blockBlob = mock(CloudBlockBlob.class);
        BlobOutputStream blobOutputStream = mock(BlobOutputStream.class);
        BlobProperties blobProperties = mock(BlobProperties.class);

        when(storageAccount.createCloudBlobClient()).thenReturn(blobClient);
        when(blockBlob.exists()).thenReturn(true);
        when(blobContainer.exists()).thenReturn(true);
        when(blockBlob.getProperties()).thenReturn(blobProperties);
        when(blobProperties.getLength()).thenReturn(4096L);
        when(blobClient.getContainerReference(eq(AZ_TEST_CONTAINER_NAME))).thenReturn(blobContainer);
        when(blobContainer.getBlockBlobReference(eq(blobName))).thenReturn(blockBlob);
        when(blobContainer.getBlockBlobReference(anyString())).thenReturn(blockBlob);
        when(blockBlob.getName()).thenReturn(blobName);
        when(blockBlob.openInputStream()).thenReturn(mock(BlobInputStream.class));
        when(blockBlob.openOutputStream()).thenReturn(mock(BlobOutputStream.class));
        when(blockBlob.openOutputStream()).thenReturn(blobOutputStream);

        return storageAccount;
    }
}
