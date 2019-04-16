package io.confluent.connect;

import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.S3SinkTask;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageFactory;
import org.apache.kafka.connect.sink.SinkTask;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.internal.MocksBehavior;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import io.confluent.connect.storage.StorageSinkConnectorConfig;

import static org.mockito.Mockito.*;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkTask.class,AzBlobStorage.class})
public class AzBlobSinkTaskTest extends AzBlobSinkConnectorTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        PowerMockito.whenNew(AzBlobStorage.class).withAnyArguments().thenReturn(storage);
//        Capture<AzBlobSinkConnectorConfig> capturedConf = EasyMock.newCapture();
//        Capture<String> capturedUrl = EasyMock.newCapture();
//        PowerMock.mockStatic(AzBlobStorage.class);
    }

    @Test
    public void testTaskType() throws Exception {
        AzBlobSinkTask task = new AzBlobSinkTask();
        SinkTask.class.isAssignableFrom(task.getClass());
    }

    @Test
    public void basic() throws Exception{
        AzBlobSinkTask task = new AzBlobSinkTask();
        task.initialize(context);
        task.start(properties);
        task.close(context.assignment());
        task.stop();
    }


}
