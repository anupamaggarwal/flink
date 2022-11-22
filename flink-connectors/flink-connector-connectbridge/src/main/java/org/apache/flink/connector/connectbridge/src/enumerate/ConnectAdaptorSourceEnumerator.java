package org.apache.flink.connector.connectbridge.src.enumerate;

import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A barebones implementation of a SourceEnumerator meant to plug source kafka connectors to
 * flink runtime
 * <p/>
 * On a high level splits are regarded as invidual tasks
 * Every task will be run on a task slot and process individual task level data
 * <p/>
 * Framework's SourceReader pollNext will be transformed to calling poll on the connect tasks.
 * SplitFetcherThreads will run these tasks on individual thread similar to connect runtime
 * <p/>
 * Any config updates /communication btw connector and tasks will be shared using Flink's custom
 * SourceEvent mechanism
 *
 * @see org.apache.kafka.connect.connector.Task
 * @see org.apache.kafka.connect.source.SourceRecord
 */
public class ConnectAdaptorSourceEnumerator
        implements SplitEnumerator<ConnectorAdaptorSplit, ConnectAdaptorEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectAdaptorSourceEnumerator.class);


    private final Map<String, String> connectorProperties;

    /**
     * task properties depends on number of tasks running and is something our enumerator generates
     */
    private List<Map<String, String>> taskProperties = new ArrayList<>();

    private SourceConnector connector;

    private final SplitEnumeratorContext<ConnectorAdaptorSplit> context;

    public ConnectAdaptorSourceEnumerator(
            Map<String, String> taskProperties,
            SplitEnumeratorContext<ConnectorAdaptorSplit> context
    ) {
        this.connectorProperties = taskProperties;
        this.context = context;
        initializeConnector();
        //todo validate for all required params , call connector start
        //todo handle validations which might fail due to some required properties not set (kafka.topics)
        connector.start(this.connectorProperties);
        this.taskProperties = connector.taskConfigs(Integer.parseInt(connectorProperties.get(
                "tasks.max")));
    }


    @Override
    public void start() {
        LOG.debug(
                "Starting Connect Adaptor source enumerator with properties {} ",
                connectorProperties
        );


    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Split request from reader  {}", subtaskId);

    }

    @Override
    public void addSplitsBack(List<ConnectorAdaptorSplit> splits, int subtaskId) {

    }


    private final void initializeConnector() {
        String className = this.connectorProperties.get("connector.class");

        /**
         * todo classloader should mimick what happens in connector
         * <p> need to investigate if thereis a way to use a custom class loader within flink
         * runtime</p>
         *
         * Instead of directly working with Connectors we might need WorkerConnector
         * @see org.apache.kafka.connect.runtime.WorkerConnector
         */

        ClassLoader savedLoader = Thread.currentThread().getContextClassLoader();
        try {
            Class<? extends Connector> klass = (Class<? extends Connector>) Class.forName(
                    className,
                    true,
                    savedLoader
            );
            this.connector = (SourceConnector) klass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Failed to instantiate connector with {}", e);
            throw new RuntimeException(e);
        }


    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to ConnectAdaptorSourceEnumerator with taskID",
                subtaskId
        );
        this.context.assignSplit(new ConnectorAdaptorSplit(
                "task-" + subtaskId,
                this.taskProperties.get(0)
        ), subtaskId);
        this.context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public ConnectAdaptorEnumState snapshotState(long checkpointId) throws Exception {
        LOG.debug(
                "Connect adaptor snapshot called with checkpoint id {}", checkpointId);
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
