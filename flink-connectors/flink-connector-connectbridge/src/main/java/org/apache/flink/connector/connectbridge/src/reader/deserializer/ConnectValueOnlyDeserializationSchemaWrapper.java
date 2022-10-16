package org.apache.flink.connector.connectbridge.src.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class which handles conversion from SourceRecord to desired O/P format of type T
 * We usually don't care about the keys
 * <p/>
 * Other information in sourceRecord will need to be converted into flink based counterparts
 * For example offsets in sourceRecord will have to be converted to offsets of some kinds which flink's checkpoint mechanism understand
 * @param <T>
 */
 public class ConnectValueOnlyDeserializationSchemaWrapper<T> implements ConnectRecordDeserializationSchema<T> {
     private static final Logger LOG = LoggerFactory.getLogger(ConnectValueOnlyDeserializationSchemaWrapper.class);
     private transient Deserializer<T> deserializer;
     private final  Class<? extends Deserializer<T>> deserializerClass;

    public ConnectValueOnlyDeserializationSchemaWrapper( Class<? extends Deserializer<T>> deserializerClass) {
        this.deserializerClass = deserializerClass;
        try {
            this.deserializer = (Deserializer<T>) Class.forName(deserializerClass.getName()).newInstance();
        } catch (Exception e) {
             LOG.warn("Failed to convert {}",e);
        }


    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws IOException {
        //we probably don't care about the key?
       T val =  this.deserializer.deserialize(record.topic(),((String)record.value()).getBytes());
       out.collect(val);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.createTypeInfo(Deserializer.class, this.deserializerClass, 0, null, null);

    }


}
