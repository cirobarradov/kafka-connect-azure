package io.confluent.connect;

import io.confluent.connect.Utils.FileUtil;
import io.confluent.connect.azblob.AzBlobSinkConnectorConfig;
import io.confluent.connect.azblob.AzBlobSinkTask;
import io.confluent.connect.azblob.format.bytearray.ByteArrayFormat;
import io.confluent.connect.azblob.storage.AzBlobStorage;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AzBlobSinkTask.class, AzBlobStorage.class})
public class DataWriterByteArrayTest extends AzBlobMocked {

    private static final String ZERO_PAD_FMT = "%010d";
    ByteArrayFormat format;
    protected Map<String, String> localProps = new HashMap<>();
    private ByteArrayConverter converter;
    protected Partitioner<FieldSchema> partitioner;
    protected AzBlobSinkTask task;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.putAll(localProps);
        return props;
    }

    public void setUp() throws Exception {
        super.setUp();
        converter = new ByteArrayConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

        partitioner = new DefaultPartitioner<>();
        partitioner.configure(parsedConfig);

        format = new ByteArrayFormat(storage);

        PowerMockito.whenNew(AzBlobStorage.class).withAnyArguments().thenReturn(storage);
        assertTrue(blobClient.getContainerReference(AZ_TEST_CONTAINER_NAME).exists());
    }

    @Test
    public void testNoSchema() throws Exception{
        localProps.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);
        List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
        task.put(sinkRecords);
        //task.close(context.assignment());
        task.stop();
        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment(), ".bin");
    }

    @Test
    public void testCustomExtensionAndLineSeparator() throws Exception {
        String extension = ".customExtensionForTest";
        localProps.put(AzBlobSinkConnectorConfig.FORMAT_CLASS_CONFIG, ByteArrayFormat.class.getName());
        localProps.put(AzBlobSinkConnectorConfig.FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG, "SEPARATOR");
        localProps.put(AzBlobSinkConnectorConfig.FORMAT_BYTEARRAY_EXTENSION_CONFIG, extension);
        setUp();
        task = new AzBlobSinkTask(connectorConfig, context, storage, partitioner, format, SYSTEM_TIME);

        List<SinkRecord> sinkRecords = createByteArrayRecordsWithoutSchema(7 * context.assignment().size(), 0, context.assignment());
        task.put(sinkRecords);
        task.close(context.assignment());
        task.stop();

        long[] validOffsets = {0, 3, 6};
        verify(sinkRecords, validOffsets, context.assignment(), extension);
    }

    public List<SinkRecord> createByteArrayRecordsWithoutSchema(int size, long startOffset, Set<TopicPartition> partitions) {
        String key = "key";
        int ibase = 12;

        List<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = startOffset, total = 0; total < size; ++offset) {
            for (TopicPartition tp : partitions) {
                byte[] record = ("{\"schema\":{\"type\":\"struct\",\"fields\":[ " +
                        "{\"type\":\"boolean\",\"optional\":true,\"field\":\"booleanField\"}," +
                        "{\"type\":\"int32\",\"optional\":true,\"field\":\"intField\"}," +
                        "{\"type\":\"int64\",\"optional\":true,\"field\":\"longField\"}," +
                        "{\"type\":\"string\",\"optional\":false,\"field\":\"stringField\"}]," +
                        "\"payload\":" +
                        "{\"booleanField\":\"true\"," +
                        "\"intField\":" + String.valueOf(ibase) + "," +
                        "\"longField\":" + String.valueOf((long) ibase) + "," +
                        "\"stringField\":str" + String.valueOf(ibase) +
                        "}}").getBytes();
                sinkRecords.add(new SinkRecord(TOPIC, tp.partition(), null, key, null, record, offset));
                if (++total >= size) {
                    break;
                }
            }
        }
        return sinkRecords;
    }

    protected void verifyFileListing(long[] validOffsets, Set<TopicPartition> partitions, String extension) throws IOException {
        List<String> expectedFiles = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            expectedFiles.addAll(getExpectedFiles(validOffsets, tp, extension));
        }
        verifyFileListing(expectedFiles);
    }

    protected void verifyFileListing(List<String> expectedFiles) throws IOException {
        List<String> summaries = listObjects();
        List<String> actualFiles = new ArrayList<>();
        for (String fileKey : summaries) {
            actualFiles.add(fileKey);
        }

        Collections.sort(actualFiles);
        Collections.sort(expectedFiles);
        assertThat(actualFiles, is(expectedFiles));
    }

    protected void verifyContents(List<SinkRecord> expectedRecords, int startIndex, Collection<Object> records)
            throws IOException{
        for (Object record : records) {
            byte[] bytes = (byte[]) record;
            SinkRecord expectedRecord = expectedRecords.get(startIndex++);
            byte[] expectedBytes = (byte[]) expectedRecord.value();
            assertArrayEquals(expectedBytes, bytes);
        }
    }

    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                          String extension) throws IOException {
        verify(sinkRecords, validOffsets, partitions, extension, false);
    }

    /**
     * Verify files and records are uploaded appropriately.
     * @param sinkRecords a flat list of the records that need to appear in potentially several files in S3.
     * @param validOffsets an array containing the offsets that map to uploaded files for a topic-partition.
     *                     Offsets appear in ascending order, the difference between two consecutive offsets
     *                     equals the expected size of the file, and last offset in exclusive.
     * @throws IOException
     */
    protected void verify(List<SinkRecord> sinkRecords, long[] validOffsets, Set<TopicPartition> partitions,
                          String extension, boolean skipFileListing)
            throws IOException {
        if (!skipFileListing) {
            verifyFileListing(validOffsets, partitions, extension);
        }
        for (TopicPartition tp : partitions) {
            for (int i = 1, j = 0; i < validOffsets.length; ++i) {
                long startOffset = validOffsets[i - 1];
                long size = validOffsets[i] - startOffset;

                FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset, extension, ZERO_PAD_FMT);
                Collection<Object> records = readRecords(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                        extension, ZERO_PAD_FMT);
                // assertEquals(size, records.size());
                verifyContents(sinkRecords, j, records);
                j += size;
            }
        }
    }

    protected String getDirectory(String topic, int partition) {
        String encodedPartition = "partition=" + String.valueOf(partition);
        return partitioner.generatePartitionedPath(topic, encodedPartition);
    }

    protected List<String> getExpectedFiles(long[] validOffsets, TopicPartition tp, String extension) {
        List<String> expectedFiles = new ArrayList<>();
        for (int i = 1; i <= validOffsets.length; ++i) {
            long startOffset = validOffsets[i - 1];
            expectedFiles.add(FileUtil.fileKeyToCommit(topicsDir, getDirectory(tp.topic(), tp.partition()), tp, startOffset,
                    extension, ZERO_PAD_FMT));
        }
        return expectedFiles;
    }
}
