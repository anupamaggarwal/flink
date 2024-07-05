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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

/**
 * A default implementation of {@link ReducingState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
public class InternalReducingState<K, V> extends InternalKeyedState<K, V>
        implements ReducingState<V> {

    protected final ReduceFunction<V> reduceFunction;

    public InternalReducingState(
            StateRequestHandler stateRequestHandler, ReducingStateDescriptor<V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
        this.reduceFunction = stateDescriptor.getReduceFunction();
    }

    @Override
    public StateFuture<V> asyncGet() {
        return handleRequest(StateRequestType.REDUCING_GET, null);
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        return handleRequest(StateRequestType.REDUCING_ADD, value);
    }
}
