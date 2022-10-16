package org.apache.flink.connector.connectbridge.src.split;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ConnectAdaptorSplitSerializerTest {

    @Test
    public void testSerDe(){

        ConnectorAdaptorSplit split = new ConnectorAdaptorSplit("test", ImmutableMap.of("k1","v1", "k2","v2"));
        ConnectAdaptorSplitSerializer serde = new ConnectAdaptorSplitSerializer();
        try {
            byte[] serialized = serde.serialize(split);
            ConnectorAdaptorSplit fromBytes = serde.deserialize(serde.getVersion(),serialized);
            assertEquals(fromBytes.getTaskId(), split.getTaskId());
            System.out.println("Printing map " + fromBytes.getTaskConfigs());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

}
