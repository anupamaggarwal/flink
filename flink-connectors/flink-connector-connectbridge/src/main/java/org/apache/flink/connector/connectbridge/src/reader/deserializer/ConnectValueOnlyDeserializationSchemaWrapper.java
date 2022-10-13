package org.apache.flink.connector.connectbridge.src.reader.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.io.IOException;

class ConnectValueOnlyDeserializationSchemaWrapper<T> implements ConnectRecordDeserializationSchema<T> {
    private final DeserializationSchema<T> deserializationSchema;
    private final Converter valueConverter;
    private final Converter keyConverter;
    private final HeaderConverter headerConverter;

    ConnectValueOnlyDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema,
                                                 Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter) {
        this.deserializationSchema = deserializationSchema;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.headerConverter = headerConverter;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws IOException {
        //todo we probably don't care about the key?
        byte[] value = valueConverter.fromConnectData(record.topic(), convertHeaderFor(record),
                record.valueSchema(), record.value());

        deserializationSchema.deserialize(value,out);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
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
}
