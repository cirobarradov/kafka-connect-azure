package io.confluent.connect;

import io.confluent.connect.s3.S3SinkConnector;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.*;

public class AzBlobSinkConnectorTest {
    @Test
    public void testVersion() {
        String version = new S3SinkConnector().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void connectorType() {
        Connector connector = new S3SinkConnector();
        assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
    }
}
