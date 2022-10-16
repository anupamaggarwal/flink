package org.apache.flink.connector.connectbridge.src;

import com.google.common.collect.ImmutableMap;

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

import java.util.Map;

public class ConnectAdaptorSource<RecordT>
        implements Source<RecordT, ConnectorAdaptorSplit, ConnectAdaptorEnumState> , ResultTypeQueryable<RecordT> {

    private final Boundedness boundedness;
    private final ConnectRecordDeserializationSchema<RecordT> deserializationSchema;


    ConnectAdaptorSource(Map<String, String> connectorProperties,
                         Boundedness boundedness,
                         ConnectRecordDeserializationSchema<RecordT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.boundedness = boundedness;

    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<RecordT, ConnectorAdaptorSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new ConnectAdaptorSourceReader<>(()->new ConnectAdaptorSplitReader()
                ,new ConnectAdaptorSourceRecordEmitter<>(deserializationSchema),
                readerContext.getConfiguration(), readerContext);
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> createEnumerator(SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext)
            throws Exception {
        //todo generate task properties and pass in the enumerator

        return new ConnectAdaptorSourceEnumerator(ImmutableMap.of(),enumContext);
    }

    @Override
    public SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> restoreEnumerator(
            SplitEnumeratorContext<ConnectorAdaptorSplit> enumContext,
            ConnectAdaptorEnumState checkpoint
    ) throws Exception {
        return new ConnectAdaptorSourceEnumerator(ImmutableMap.of(),enumContext);
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
