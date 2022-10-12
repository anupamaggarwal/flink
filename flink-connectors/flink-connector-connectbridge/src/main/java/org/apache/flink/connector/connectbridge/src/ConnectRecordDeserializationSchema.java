package org.apache.flink.connector.connectbridge.src;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.kafka.connect.source.SourceRecord;


import java.io.IOException;
import java.io.Serializable;

public interface ConnectRecordDeserializationSchema<T> extends Serializable,
        ResultTypeQueryable<T> {
    /**
     * Initialization method for the schema. It is called before the actual working methods {@link
     * #deserialize} and thus suitable for one time setup work.
     *
     * <p>The provided
     * {@link org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext} can be used to access
     * additional features such as e.g. registering user metrics.
     *
     * @param context Contextual information that can be used during initialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}


    /**
     * Deserializes the kafka connect record.
     *
     * <p>Can output multiple records through the {@link Collector}. Note that number and size of
     * the produced records should be relatively small. Depending on the source implementation
     * records can be buffered in memory or collecting records might delay emitting checkpoint
     * barrier.
     *
     * @param record The connect source record to deserialize.
     * @param out The collector to put the resulting messages.
     */
    void deserialize(SourceRecord record, Collector<T> out) throws IOException;
}
