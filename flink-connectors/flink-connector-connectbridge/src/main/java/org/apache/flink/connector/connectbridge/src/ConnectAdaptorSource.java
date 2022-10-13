package org.apache.flink.connector.connectbridge.src;

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
import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import java.util.Map;

public class ConnectAdaptorSource<OUT>
        implements Source<OUT, ConnectorAdaptorSplit, ConnectAdaptorEnumState> , ResultTypeQueryable<OUT> {

    private final Boundedness boundedness;
    private final ConnectRecordDeserializationSchema<OUT> deserializationSchema;


    ConnectAdaptorSource(Map<String, String> connectorProperties,
                         Boundedness boundedness,
                         ConnectRecordDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.boundedness = boundedness;

    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, ConnectorAdaptorSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> createEnumerator(SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext)
            throws Exception {
        //todo generate task properties and pass in the enumerator

        return new ConnectAdaptorSourceEnumerator(ImmutableList.of(),enumContext);
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> restoreEnumerator(
            SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext,
            ConnectAdaptorEnumState checkpoint
    ) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<ConnectorAdaptorSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<ConnectAdaptorEnumState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return null;
    }

}
