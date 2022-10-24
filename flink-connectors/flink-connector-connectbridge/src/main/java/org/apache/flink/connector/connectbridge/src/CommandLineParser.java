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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * CLI parser for parsing params for Connector bridge.
 */
public class CommandLineParser extends ExecutionConfig.GlobalJobParameters {

    public static final String ITERATIONS = "iterations";
    public static final String EXECUTION_MODE = "execution-mode";

    public static final String KAFKA_OUTPUT = "kafka-output";

    public static CommandLineParser fromArgs(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        int iterations=10;
        if (params.has(ITERATIONS)) {
            iterations = Integer.parseInt(params.get(ITERATIONS));
        } else {
            System.out.println("Executing example with default iterations of 10 data points");
        }
        boolean kafkaOuptut = false;

        RuntimeExecutionMode executionMode = ExecutionOptions.RUNTIME_MODE.defaultValue();
        if (params.has(EXECUTION_MODE)) {
            executionMode = RuntimeExecutionMode.valueOf(params.get(EXECUTION_MODE).toUpperCase());
        }else {
            System.out.println("Executing with default execution mode of batch ");
            executionMode = RuntimeExecutionMode.BATCH;

        }

        if (params.has(KAFKA_OUTPUT)) {
            kafkaOuptut = Boolean.parseBoolean(params.get(KAFKA_OUTPUT));
            System.out.println("Executing with kafka output " + kafkaOuptut);
        }else {
            kafkaOuptut = false;
            System.out.println("Executing with kafka output " + kafkaOuptut);

        }

        return new CommandLineParser(iterations, executionMode, params, kafkaOuptut);
    }

    private final RuntimeExecutionMode executionMode;
    private final MultipleParameterTool params;
    private final boolean outputToKafka;

    public boolean shouldOutputToKafka() {
        return outputToKafka;
    }

    private CommandLineParser(
            int iterations,
            RuntimeExecutionMode executionMode,
            MultipleParameterTool params,
            boolean kafkaOuptut
    ) {
        this.iterations = iterations;
        this.executionMode = executionMode;
        this.params = params;
        this.outputToKafka= kafkaOuptut;
    }

    private int iterations;

    public Optional<Integer> getIterations() {
        return Optional.ofNullable(iterations);
    }

    public RuntimeExecutionMode getExecutionMode() {
        return executionMode;
    }

    public OptionalInt getInt(String key) {
        if (params.has(key)) {
            return OptionalInt.of(params.getInt(key));
        }

        return OptionalInt.empty();
    }

    @Override
    public Map<String, String> toMap() {
        return params.toMap();
    }

}
