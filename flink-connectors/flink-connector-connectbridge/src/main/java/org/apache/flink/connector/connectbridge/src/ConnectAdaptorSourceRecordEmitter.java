/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.connectbridge.src.enumerate.ConnectAdaptorEnumState;

import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.function.Function;


@Internal
final class ConnectAdaptorSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecord, T, ConnectAdaptorEnumState> {
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    private final ConnectRecordDeserializationSchema<T> deserializer;
    public ConnectAdaptorSourceRecordEmitter(ConnectRecordDeserializationSchema<T> deserializer){
        this.deserializer = deserializer;
    }


    @Override
    public void emitRecord(
            SourceRecord element,
            SourceOutput<T> output,
            ConnectAdaptorEnumState splitState
    ) throws Exception {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            deserializer.deserialize(element, sourceOutputWrapper);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize consumer record due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;
        private long timestamp;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
