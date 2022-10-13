package org.apache.flink.connector.connectbridge.src.split;

import org.apache.flink.api.connector.source.SourceSplit;

public class ConnectorAdaptorSplit implements SourceSplit {


    private final String taskId ;
    public ConnectorAdaptorSplit(String taskId){
        this.taskId = taskId;
    }


    @Override
    public String splitId() {
        return taskId;
    }
}
