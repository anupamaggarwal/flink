package org.apache.flink.connector.connectbridge.src.reader.deserializer;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public interface ConnectRecordDeserializationSchema<T> extends Serializable,
        ResultTypeQueryable<T> {

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




    static <V> ConnectRecordDeserializationSchema<V> valueOnly(
            Class<? extends Deserializer<V>> valueDeserializerClass) {
        return valueOnly(valueDeserializerClass, Collections.emptyMap());
    }


    static <V, D extends Deserializer<V>> ConnectRecordDeserializationSchema<V> valueOnly(
            Class<D> valueDeserializerClass, Map<String, String> config) {
        return new ConnectValueOnlyDeserializationSchemaWrapper<>(valueDeserializerClass);
    }

}
