package org.apache.flink.connector.connectbridge.src;


import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorEnumState;

import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;

public class ConnectAdaptorSplitReader implements SplitReader<SourceRecord, ConnectorAdaptorSplit> {
    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ConnectorAdaptorSplit> splitsChanges) {

    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
