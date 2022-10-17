package org.apache.flink.connector.connectbridge.src.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.Serializable;


/**
 * Deserializes the kafka connect record.
 *
 * <p>Can output multiple records through the {@link Collector}. Note that number and size of
 * the produced records should be relatively small. Depending on the source implementation
 * records can be buffered in memory or collecting records might delay emitting checkpoint
 * barrier.
 *
 */

public  interface ConnectRecordDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    void deserialize(SourceRecord record, Collector<T> out) throws IOException;

}
