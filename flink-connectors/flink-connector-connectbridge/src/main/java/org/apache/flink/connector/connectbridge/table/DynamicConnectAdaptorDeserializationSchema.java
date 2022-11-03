package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.connectbridge.src.reader.deserializer.ConnectRecordDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DynamicConnectAdaptorDeserializationSchema implements ConnectRecordDeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(
            DynamicConnectAdaptorDeserializationSchema.class);
    private String valueConverterKlass;
    //for now we don't really care about the key part much
    private DeserializationSchema<RowData> keyDeserialization;
    private DeserializationSchema<RowData> valueDeserialization;
    private transient Converter converter;


    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }
    public DynamicConnectAdaptorDeserializationSchema(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            String valueConverterKlass
    ) {
        try {
            this.valueConverterKlass = valueConverterKlass;
            this.keyDeserialization = keyDeserialization;
            this.valueDeserialization = valueDeserialization;
            converter = (Converter) Class.forName(valueConverterKlass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error in de-serializing {}", e);
        }
    }


    @Override
    public void deserialize(SourceRecord record, Collector<RowData> out) throws IOException {
        try {
            if (converter == null) {
                converter = (Converter) Class.forName(valueConverterKlass).newInstance();
                Map<String,String> configs = new HashMap<>();
                //todo - clean this up , ideally this should come from user config for now this is harcoded
                configs.put("schemas.enable","false");
                converter.configure(configs, false);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error in de-serializing {}", e);
        }

        byte[] data = converter.fromConnectData(
                record.topic(),
                record.valueSchema(),
                record.value()
        );
        String jsonValue = new String(data);
        LOG.debug("Value of json {}",jsonValue);
        RowData value = valueDeserialization.deserialize(data);
        out.collect(value);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
