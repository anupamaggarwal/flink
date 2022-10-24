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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


@Internal
final class ConnectAdaptorSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecord, T, ConnectAdaptorEnumState> {
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    private final ConnectRecordDeserializationSchema<T> deserializer;
    private final boolean shouldOutputToKafka;
    private final Map<String,String> connectorConfigs;

    private KafkaProducer<byte[], byte[]> producer;
    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;
    private static final Logger LOG = LoggerFactory.getLogger(ConnectAdaptorSourceRecordEmitter.class);


    public ConnectAdaptorSourceRecordEmitter(ConnectRecordDeserializationSchema<T> deserializer,
                                             Map<String, String> connectorConfigs,

                                             boolean outputToKafka
    ){
        this.deserializer = deserializer;
        this.shouldOutputToKafka = outputToKafka;
        this.connectorConfigs = connectorConfigs;
        //initialize a kafka producer
        if(shouldOutputToKafka)
            initalizeProducer(connectorConfigs);
    }

    private void initalizeProducer(Map<String, String> connectorConfigs) {
        //todo - need to handle config overrides

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.putAll(connectorConfigs);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectorConfigs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, connectorConfigs.get(ProducerConfig.CLIENT_ID_CONFIG));
        LOG.debug("Using producer props {}", producerProps);
         //add client metrics.context properties
       producer = new KafkaProducer<>(producerProps);
       initializeConverters();

    }

    private void initializeConverters() {
        ClassLoader savedLoader = Thread.currentThread().getContextClassLoader();
        //todo defaults
        this.keyConverter = initializeClass(connectorConfigs.get("key.converter"),savedLoader);
        keyConverter.configure(connectorConfigs,false);

        this.valueConverter = initializeClass(connectorConfigs.get("value.converter"),savedLoader);
        valueConverter.configure(new HashMap(),false);
        this.headerConverter = initializeClass(connectorConfigs.get("header.converter"),savedLoader);
        valueConverter.configure(connectorConfigs,false);

    }

    private <ClassT> ClassT initializeClass(String className, ClassLoader cl){
        try {
            Class<? extends ClassT> klass = (Class<? extends ClassT>) Class.forName(
                    className,
                    true,
                    cl
            );
            ClassT instance =  klass.newInstance();
            return instance;
            //need to call initialize with fake context
           } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Failed to instantiate task with {}", e);
            throw new RuntimeException(e);
        }
    }


    protected RecordHeaders convertHeaderFor(SourceRecord record) {
        Headers headers = record.headers();
        RecordHeaders result = new RecordHeaders();
        if (headers != null) {
            String topic = record.topic();
            for (Header header : headers) {
                String key = header.key();
                byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                result.add(key, rawHeader);
            }
        }
        return result;
    }


    /**
     * Convert the source record into a producer record.
     *
     * @param record the transformed record
     * @return the producer record which can sent over to Kafka. A null is returned if the input is null or
     * if an error was encountered during any of the converter stages.
     */
    protected ProducerRecord<byte[], byte[]> convertTransformedRecord(SourceRecord record) {
        if (record == null) {
            return null;
        }

        RecordHeaders headers = convertHeaderFor(record);

        byte[] key = keyConverter.fromConnectData(record.topic(), headers, record.keySchema(), record.key());
        valueConverter.configure(new HashMap(),false);
        byte[] value = valueConverter.fromConnectData(record.topic(), headers, record.valueSchema(), record.value());

        return new ProducerRecord<>(record.topic(), record.kafkaPartition(),
                checkAndConvertTimestamp(record.timestamp()), key, value, headers);
    }

    public static Long checkAndConvertTimestamp(Long timestamp) {
        if (timestamp == null || timestamp >= 0)
            return timestamp;
        else if (timestamp == RecordBatch.NO_TIMESTAMP)
            return null;
        else
            throw new InvalidRecordException(String.format("Invalid record timestamp %d", timestamp));
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
            if(shouldOutputToKafka)
                producer.send(convertTransformedRecord(element));
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
