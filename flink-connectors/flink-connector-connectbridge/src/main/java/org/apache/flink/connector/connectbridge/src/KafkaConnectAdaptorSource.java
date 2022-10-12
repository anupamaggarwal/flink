package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Map;

public class KafkaConnectAdaptorSource<OUT, SplitT extends SourceSplit, EnumCheckT>
        implements Source<OUT, SplitT, EnumCheckT> , ResultTypeQueryable<OUT> {



     KafkaConnectAdaptorSource(Map<String, String> connectorProperties,
                                     Boundedness boundedness) {

    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<SplitT, EnumCheckT> createEnumerator(SplitEnumeratorContext<SplitT> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<SplitT, EnumCheckT> restoreEnumerator(
            SplitEnumeratorContext<SplitT> enumContext,
            EnumCheckT checkpoint
    ) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<EnumCheckT> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return null;
    }

}
