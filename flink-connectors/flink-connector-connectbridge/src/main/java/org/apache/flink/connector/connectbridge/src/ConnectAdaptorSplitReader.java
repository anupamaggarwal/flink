package org.apache.flink.connector.connectbridge.src;


import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.connectbridge.src.split.ConnectorAdaptorSplit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;

public class ConnectAdaptorSplitReader implements SplitReader<SourceRecord, ConnectorAdaptorSplit> {

    private boolean shouldContinue = true;

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        if (shouldContinue) {
            shouldContinue = false;
            return new ConnectAdaptorRecord();
        } else {
            return null;//
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<ConnectorAdaptorSplit> splitsChanges) {
        //need to handle this event and add some state obtained from the split
        //typically this could be a task conf
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }


    public static class ConnectAdaptorRecord implements RecordsWithSplitIds<SourceRecord> {
        private int recordCounter = 0;


        public ConnectAdaptorRecord() {

        }

        @Nullable
        @Override
        public String nextSplit() {
            if(recordCounter>=5)
                return null;
            else
            return "task-0";
        }

        @Nullable
        @Override
        public SourceRecord nextRecordFromSplit() {
            //call task with new source record
            while (recordCounter < 5) {
                 SourceRecord record1 = new SourceRecord(
                        emptyMap(),
                        emptyMap(),
                        "DUMMY",
                        0,
                        Schema.STRING_SCHEMA,
                        "this is val"
                );
                recordCounter++;
                return record1;
            }

            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            Set<String> res = new HashSet<>();
            res.add("task-0");
            return res;
        }
    }
}
