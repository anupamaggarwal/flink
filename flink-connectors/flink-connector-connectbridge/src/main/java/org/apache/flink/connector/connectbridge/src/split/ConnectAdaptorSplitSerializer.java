package org.apache.flink.connector.connectbridge.src.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class ConnectAdaptorSplitSerializer implements SimpleVersionedSerializer<ConnectorAdaptorSplit> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(ConnectorAdaptorSplit split) throws IOException {
        return split.splitId().getBytes();
    }

    @Override
    public ConnectorAdaptorSplit deserialize(
            int version,
            byte[] serialized
    ) throws IOException {
        return new ConnectorAdaptorSplit(new String(serialized));
    }
}
