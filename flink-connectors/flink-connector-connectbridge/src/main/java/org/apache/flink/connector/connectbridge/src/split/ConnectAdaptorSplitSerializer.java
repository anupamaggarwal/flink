package org.apache.flink.connector.connectbridge.src.split;

import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorSourceEnumerator;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ConnectAdaptorSplitSerializer implements
        SimpleVersionedSerializer<ConnectorAdaptorSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectAdaptorSplitSerializer.class);

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(ConnectorAdaptorSplit split) throws IOException {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream ob = new ObjectOutputStream(baos);
        ) {
            ob.writeUTF(split.getTaskId());
            ob.writeObject(split.getTaskConfigs());
            ob.flush();
            return baos.toByteArray();
        }

    }

    @Override
    public ConnectorAdaptorSplit deserialize(
            int version,
            byte[] serialized
    ) throws IOException {
        try (
                ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream in = new ObjectInputStream(bais)) {


            try {
                String taskId = in.readUTF();
                Map<String,String> properties = null;
                properties = (Map<String, String>) in.readObject();
                return new ConnectorAdaptorSplit(taskId,properties);
            } catch (ClassNotFoundException e) {
                LOG.error("Cannot deserialize properly");
                throw new RuntimeException("Error in deserialization",e);
            }
        }
    }
}
