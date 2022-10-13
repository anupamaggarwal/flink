package org.apache.flink.connector.connectbridge.src.enumerate;

import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ConnectAdaptorSourceEnumerator
        implements SplitEnumerator<ConnectorAdaptorSplit,ConnectAdaptorEnumState> {


    private final List<Map<String,String>> taskProperties;
    private final SplitEnumeratorContext<ConnectorAdaptorSplit> context;

    public ConnectAdaptorSourceEnumerator(List<Map<String,String>> taskProperties,
                                          SplitEnumeratorContext<ConnectorAdaptorSplit> context){
        this.taskProperties = taskProperties;
        this.context = context;
    }


    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<ConnectorAdaptorSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public ConnectAdaptorEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
