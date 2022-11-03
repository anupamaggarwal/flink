package org.apache.flink.connector.connectbridge.src;

import com.google.common.collect.ImmutableMap;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorEnumState;
import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorSourceEnumerator;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;
import org.apache.flink.connector.connectbridge.src.split.ConnectAdaptorSplitSerializer;
import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Map;

/**
 * Class which serves as as adaptor to Kafka connect connectors
 * <p/>
 * This is the main class which tries to bridge flink to enable it to run kafka connectors
 *
 * @param <RecordT>
 */
public class ConnectAdaptorSource<RecordT>
        implements Source<RecordT, ConnectorAdaptorSplit, ConnectAdaptorEnumState>,
        ResultTypeQueryable<RecordT> {

    private final Boundedness boundedness;
    private final Map<String, String> connectorConfigs;
    private final ConnectRecordDeserializationSchema<RecordT> deserializationSchema;
    private final boolean outputToKafka;
    private TypeInformation<RecordT> tTypeInformation;


    ConnectAdaptorSource(
            Map<String, String> connectorProperties,
            Boundedness boundedness,
            ConnectRecordDeserializationSchema<RecordT> deserializer,
            boolean outputToKafka
    ) {

        //check for required params per connector type
        this.deserializationSchema = deserializer;
        this.boundedness = boundedness;
        this.connectorConfigs = connectorProperties;
        this.outputToKafka = outputToKafka;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<RecordT, ConnectorAdaptorSplit> createReader(SourceReaderContext readerContext) throws Exception {
        //deserializationSchema.open(readerContext);

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });
        return new ConnectAdaptorSourceReader<>(() -> new ConnectAdaptorSplitReader()
                , new ConnectAdaptorSourceRecordEmitter<>(deserializationSchema, connectorConfigs, outputToKafka),
                readerContext.getConfiguration(), readerContext
        );
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> createEnumerator(
            SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext
    )
            throws Exception {
        return new ConnectAdaptorSourceEnumerator(this.connectorConfigs, enumContext);
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> restoreEnumerator(
            SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext,
            ConnectAdaptorEnumState checkpoint
    ) throws Exception {
        return new ConnectAdaptorSourceEnumerator(ImmutableMap.of(), enumContext);
    }

    @Override
    public SimpleVersionedSerializer<ConnectorAdaptorSplit> getSplitSerializer() {
        return new ConnectAdaptorSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<ConnectAdaptorEnumState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<RecordT> getProducedType() {
         return deserializationSchema.getProducedType();
    }

}
