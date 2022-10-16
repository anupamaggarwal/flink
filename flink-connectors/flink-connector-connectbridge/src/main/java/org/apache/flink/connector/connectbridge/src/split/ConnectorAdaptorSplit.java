package org.apache.flink.connector.connectbridge.src.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Map;

public class ConnectorAdaptorSplit implements SourceSplit {


    public String getTaskId() {
        return taskId;
    }

    public Map<String, String> getTaskConfigs() {
        return taskConfigs;
    }

    private final String taskId ;
    private final Map<String,String> taskConfigs;
    public ConnectorAdaptorSplit(String taskId, Map<String,String> taskConfigs){
        this.taskId = taskId;
        this.taskConfigs = taskConfigs;
    }


    @Override
    public String splitId() {
        return taskId;
    }
}
