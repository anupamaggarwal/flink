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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectValueOnlyDeserializationSchemaWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.util.Map;

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
        if(params.getIterations().isPresent())
            iterations = params.getIterations().get();

        Map<String, String> DATAGEN_CONFIG = ImmutableMap.<String, String>builder()
                .put("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector")
                .put("connector.task.class", "io.confluent.kafka.connect.datagen.DatagenTask")
                .put("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .put("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .put("max.interval", "1000")
                .put("kafka.topic", "Foobar")
                .put("quickstart", "users")
                .put("iterations", Integer.toString(iterations))
                .put("tasks.max", "1")
                .build();

        ConnectAdaptorSource<String> source =
                new ConnectAdaptorSourceBuilder<String>(DATAGEN_CONFIG).setDeserializer(new ConnectValueOnlyDeserializationSchemaWrapper<>(
                                StringDeserializer.class,DATAGEN_CONFIG.get("value.converter"))).
                        setBounded(Boundedness.BOUNDED).build();

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

       DataStream<String> records =  stream.map(jsonRecord -> {

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
