package org.apache.flink.connector.connectbridge.src.enumerate;

import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConnectAdaptorSourceEnumerator
        implements SplitEnumerator<ConnectorAdaptorSplit,ConnectAdaptorEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectAdaptorSourceEnumerator.class);


    private final Map<String,String> connectorProperties;

    /**
     * task properties depends on number of tasks running and is something our enumerator generates
     *
     */
    private List<Map<String,String>> taskProperties = new ArrayList<>();

    private final SplitEnumeratorContext<ConnectorAdaptorSplit> context;

    public ConnectAdaptorSourceEnumerator(Map<String,String> taskProperties,
                                          SplitEnumeratorContext<ConnectorAdaptorSplit> context){
        this.connectorProperties = taskProperties;
        this.context = context;
    }


    @Override
    public void start() {
        LOG.debug("Starting Connect Adaptor source enumerator ");
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<ConnectorAdaptorSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to ConnectAdaptorSourceEnumerator with taskID",
                subtaskId);
    }

    @Override
    public ConnectAdaptorEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
