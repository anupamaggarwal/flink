package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;

import java.util.Map;

public class ConnectAdaptorSourceBuilder<OUT>{

    final Map<String,String> connectorProperties;
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    private ConnectRecordDeserializationSchema<OUT> deserializationSchema = null;

    public ConnectAdaptorSourceBuilder(Map<String,String> connectorProperties){
        this.connectorProperties = connectorProperties;

    }

    public ConnectAdaptorSourceBuilder<OUT> setBounded(Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }
    public ConnectAdaptorSourceBuilder<OUT> setDeserializer(
            ConnectRecordDeserializationSchema<OUT> recordDeserializer) {
        this.deserializationSchema = recordDeserializer;
        return this;
    }

    public ConnectAdaptorSource build(){
        return new ConnectAdaptorSource(connectorProperties,
                boundedness,deserializationSchema);
    }
}
