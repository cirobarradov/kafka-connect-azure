package io.confluent.connect;

import io.confluent.connect.azblob.AzBlobSinkConnector;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkConnector;
import org.junit.Test;

import static org.junit.Assert.*;

public class AzBlobSinkConnectorTest {
    @Test
    public void testVersion() {
        String version = new AzBlobSinkConnector().version();
        assertNotNull(version);
        assertFalse(version.isEmpty());
    }

    @Test
    public void connectorType() {
        Connector connector = new AzBlobSinkConnector();
        assertTrue(SinkConnector.class.isAssignableFrom(connector.getClass()));
    }
}
