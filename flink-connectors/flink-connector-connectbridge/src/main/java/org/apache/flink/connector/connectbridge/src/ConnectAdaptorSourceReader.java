package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorEnumState;
import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;
import java.util.function.Supplier;

public class ConnectAdaptorSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<
        SourceRecord, T, ConnectorAdaptorSplit, ConnectAdaptorEnumState> {


    public ConnectAdaptorSourceReader(
            Supplier<SplitReader<SourceRecord, ConnectorAdaptorSplit>> splitReaderSupplier,
            RecordEmitter<SourceRecord, T, ConnectAdaptorEnumState> recordEmitter,
            Configuration config,
            SourceReaderContext context
    ) {
        super(splitReaderSupplier, recordEmitter, config, context);
    }

    @Override
    protected void onSplitFinished(Map<String, ConnectAdaptorEnumState> finishedSplitIds) {

    }

    @Override
    protected ConnectAdaptorEnumState initializedState(ConnectorAdaptorSplit split) {
        return null;
    }

    @Override
    protected ConnectorAdaptorSplit toSplitType(
            String splitId,
            ConnectAdaptorEnumState splitState
    ) {
        return null;
    }
}
