package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;

import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;

import java.util.Map;

public class ConnectAdaptorSourceBuilder<OUT>{

    final Map<String,String> connectorProperties;
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    private ConnectRecordDeserializationSchema<OUT>  deserializer = null;

    private boolean outputToKafka=false;

    private TypeInformation<OUT>typeInformation;

    public ConnectAdaptorSourceBuilder(Map<String,String> connectorProperties){
        this.connectorProperties = connectorProperties;

    }

    public ConnectAdaptorSourceBuilder<OUT> setBounded(Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }
    public ConnectAdaptorSourceBuilder<OUT> setDeserializer(
            ConnectRecordDeserializationSchema<OUT>  deserializer ) {
        this.deserializer = deserializer;
        return this;
    }

    public ConnectAdaptorSourceBuilder<OUT> outputToKafka(boolean outputToKafka){
        this.outputToKafka = outputToKafka;
        return this;
    }



    public ConnectAdaptorSource build(){
        return new ConnectAdaptorSource(connectorProperties,
                boundedness, deserializer, outputToKafka
        );
    }
}
