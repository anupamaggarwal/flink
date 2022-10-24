/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectValueOnlyDeserializationSchemaWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

/**
 * Demo example of how to run a kafka connector using flink runtime
 */
public class ConnectorBridgeMain {


    public static void main(String[] args) throws Exception {
        final CommandLineParser params = CommandLineParser.fromArgs(args);

        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(params.getExecutionMode());
        env.getConfig();
        int iterations = 10;
        if (params.getIterations().isPresent()) {
            iterations = params.getIterations().get();
        }

        Map<String, String> DATAGEN_CONFIG = ImmutableMap.<String, String>builder()
                .put("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector")
                .put("connector.task.class", "io.confluent.kafka.connect.datagen.DatagenTask")
                .put("header.converter", "org.apache.kafka.connect.storage.StringConverter")
                .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .put("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .put("max.interval", "1000")
                .put("kafka.topic", "topic_1")
                .put("quickstart", "users")
                .put("iterations", Integer.toString(iterations))
                .put("tasks.max", "1")
                .build();


        Map<String, String> KAFKA_WORKER_PROPERTIES = ImmutableMap.<String, String>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092")
                .put(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer"
                )
                .put(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer"
                )
                .put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
                .put(GROUP_ID_CONFIG, "kafka-producer")
                .put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .put(ProducerConfig.CLIENT_ID_CONFIG, "test")
                .build();

        Map<String, String> connectorProps = new HashMap<>(DATAGEN_CONFIG);
        connectorProps.putAll(KAFKA_WORKER_PROPERTIES);

        ConnectAdaptorSource<String> source = null;
        if (params.shouldOutputToKafka()) {
            source = new ConnectAdaptorSourceBuilder<String>(connectorProps)
                    .setDeserializer(new ConnectValueOnlyDeserializationSchemaWrapper<>(
                            StringDeserializer.class, DATAGEN_CONFIG.get("value.converter")))
                    .outputToKafka(true)
                    .setBounded(Boundedness.BOUNDED)
                    .build();
        } else {
            source = new ConnectAdaptorSourceBuilder<String>(connectorProps)
                    .setDeserializer(new ConnectValueOnlyDeserializationSchemaWrapper<>(
                            StringDeserializer.class, DATAGEN_CONFIG.get("value.converter")))
                    .outputToKafka(false)
                    .setBounded(Boundedness.BOUNDED)
                    .build();
        }

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

        DataStream<String> records = stream.map(jsonRecord -> {

            JSONObject json = new JSONObject(jsonRecord);
            String s = json.get("payload").toString();
            return s;
        });

        records.print().name("sinkPrint");
        env.execute("ConnectorBridge");


    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
