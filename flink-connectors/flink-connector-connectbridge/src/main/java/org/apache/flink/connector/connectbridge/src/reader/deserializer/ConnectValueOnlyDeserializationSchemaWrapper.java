package org.apache.flink.connector.connectbridge.src.reader.deserializer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Class which handles conversion from SourceRecord to desired O/P format of type T
 * We usually don't care about the keys
 * <p/>
 * Other information in sourceRecord will need to be converted into flink based counterparts
 * For example offsets in sourceRecord will have to be converted to offsets of some kinds which flink's checkpoint mechanism understand
 * @param <T>
 */
 public class ConnectValueOnlyDeserializationSchemaWrapper<T>
        implements ConnectRecordDeserializationSchema<T>
{


     private static final Logger LOG = LoggerFactory.getLogger(ConnectValueOnlyDeserializationSchemaWrapper.class);
    private final Class<? extends Deserializer<T>> deserializerClass;
    private  String valueConverterKlass;
    private  transient  Converter converter;

    private  transient Deserializer<T> deserializer;
    public ConnectValueOnlyDeserializationSchemaWrapper( Class<? extends Deserializer<T>> deserializerClass,
                                                         String valueConverterKlass) {
        this.deserializerClass = deserializerClass;
        try {
            this.valueConverterKlass = valueConverterKlass;
            deserializer = deserializerClass.newInstance();
            converter = (Converter) Class.forName(valueConverterKlass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error in de-serializing {}",e);
        }
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws IOException {

        try {
            if(deserializer==null)
                deserializer = deserializerClass.newInstance();
            if(converter==null){
                converter = (Converter) Class.forName(valueConverterKlass).newInstance();
                converter.configure(new HashMap(),false);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error in de-serializing {}",e);
        }



        byte[] data = converter.fromConnectData(record.topic(),record.valueSchema(),record.value());
        T value = deserializer.deserialize(record.topic(),data);
        out.collect(value);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.createTypeInfo(Deserializer.class, deserializerClass, 0, null, null);
    }
}
