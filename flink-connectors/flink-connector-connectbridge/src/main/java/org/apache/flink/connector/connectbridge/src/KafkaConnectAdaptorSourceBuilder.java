package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.connector.source.Boundedness;

import java.util.Map;

public class KafkaConnectAdaptorSourceBuilder<OUT>{

    final Map<String,String> connectorProperties;
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;

    public KafkaConnectAdaptorSourceBuilder(Map<String,String> connectorProperties){
        this.connectorProperties = connectorProperties;

    }

    public KafkaConnectAdaptorSourceBuilder<OUT> setBounded(Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }


    public KafkaConnectAdaptorSource build(){
        return new KafkaConnectAdaptorSource(connectorProperties,boundedness);
    }
}
