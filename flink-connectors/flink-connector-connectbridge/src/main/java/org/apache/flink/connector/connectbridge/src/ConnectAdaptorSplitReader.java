package org.apache.flink.connector.connectbridge.src;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Collections.emptyMap;

public class ConnectAdaptorSplitReader implements SplitReader<SourceRecord, ConnectorAdaptorSplit> {

    private boolean shouldContinue = true;
    private Map<String, String> taskProperties;
    private String taskId;

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        if (shouldContinue) {
            shouldContinue = false;
            return new ConnectAdaptorRecordGenerator(this.taskProperties, this.taskId);
        } else {
            return null;//
        }
    }

    /**
     * Hook for handling split addition events
     * In case of connect adaptor atmost one split can be passed in for every split reader.
     * Split passed in contains context information (task properties) which is used to initialize
     * task
     * <p>
     * <p/>
     * We depend on task.class for initializing tasks
     *
     * @param splitsChanges the split changes that the SplitReader needs to handle.
     */
    @Override
    public void handleSplitsChanges(SplitsChange<ConnectorAdaptorSplit> splitsChanges) {
        //need to handle this event and add some state obtained from the split
        //typically this could be a task conf
        assert (splitsChanges.splits().size() == 1);
        ConnectorAdaptorSplit taskSplit = splitsChanges.splits().get(0);
        this.taskProperties = taskSplit.getTaskConfigs();
        this.taskId = taskSplit.getTaskId();
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }

    static class FlinkKafkaConnectorContext implements SourceTaskContext {

        public FlinkKafkaConnectorContext(Map<String, String> props) {
            this.taskProperties = props;
        }

        private final Map<String, String> taskProperties;

        @Override
        public Map<String, String> configs() {
            return taskProperties;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            //todo lets see if this passes
            return new OffsetStorageReader() {
                @Override
                public <T> Map<String, Object> offset(Map<String, T> partition) {
                    return null;
                }

                @Override
                public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                        Collection<Map<String
                                , T>> partitions
                ) {
                    return null;
                }
            };
        }
    }

    public static class ConnectAdaptorRecordGenerator
            implements RecordsWithSplitIds<SourceRecord> {
        private final String taskId;
        private int recordCounter = 0;
        private final Map<String, String> taskProperties;
        private SourceTask connectorTaskInstance;
        private final Queue<SourceRecord> queueRecords = new ConcurrentLinkedQueue<>();
        private static final Logger LOG =
                LoggerFactory.getLogger(ConnectAdaptorRecordGenerator.class);

        private boolean pollCalled = false;

        public ConnectAdaptorRecordGenerator(Map<String, String> taskProperties, String taskId) {
            this.taskProperties = taskProperties;
            this.taskId = taskId;
            initializeTask();
        }


        private void initializeTask() {
            //assert that connector.task.class is present
            ClassLoader savedLoader = Thread.currentThread().getContextClassLoader();
            String className = taskProperties.get("connector.task.class");
            try {
                Class<? extends SourceTask> klass = (Class<? extends SourceTask>) Class.forName(
                        className,
                        true,
                        savedLoader
                );
                this.connectorTaskInstance = (SourceTask) klass.newInstance();
                //need to call initialize with fake context
                connectorTaskInstance.initialize(new FlinkKafkaConnectorContext(taskProperties));
                connectorTaskInstance.start(taskProperties);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                LOG.error("Failed to instantiate task with {}", e);
                throw new RuntimeException(e);
            }
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (pollCalled && queueRecords.isEmpty()) {
                return null;
            } else {
                return taskId;
            }
        }

        @Nullable
        @Override
        public SourceRecord nextRecordFromSplit() {

            try {
                List<SourceRecord> records = connectorTaskInstance.poll();
                pollCalled=true;
                if (records != null) {
                    queueRecords.addAll(records);
                }
            } catch (Exception e) {
               // LOG.error("Error while retrieving from task ", e);
                //need to stop now
            }
            if (!queueRecords.isEmpty()) {
                return queueRecords.poll();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return ImmutableSet.of(taskId);
        }
    }
}
