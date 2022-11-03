package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.connectbridge.src.ConnectAdaptorSource;
import org.apache.flink.connector.connectbridge.src.ConnectAdaptorSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;


/**
 * Starting with absolute minimum required to make a query work
 */
public class ConnectAdaptorDynamicTableSource implements DynamicTableSource, ScanTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectAdaptorDynamicTableSource.class);
    private final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;
    private final DataType producedDataType;
    private final String asSumaryString;
    private final Map<String, String> configs;

    public ConnectAdaptorDynamicTableSource(
            Map<String, String> connectorConfigs, DataType physicalDataType,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            String asSummaryString
    ) {
        this.configs = connectorConfigs;
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat = valueDecodingFormat;
        this.producedDataType = physicalDataType;
        this.asSumaryString = asSummaryString;
        LOG.debug("Summary status {}", asSummaryString);

    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return asSumaryString;
    }

    @Override
    public ChangelogMode getChangelogMode() {
         return valueDecodingFormat.getChangelogMode();
    }



    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format) {
        if (format == null) {
            return null;
        }
        return format.createRuntimeDecoder(context, producedDataType);
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(context, keyDecodingFormat);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final ConnectAdaptorSource<RowData> kafkaConnectSource =
                createConnectAdaptorSource(keyDeserialization, valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {

                DataStreamSource<RowData> sourceStream =       execEnv.fromSource(
                                kafkaConnectSource,WatermarkStrategy.noWatermarks()
                                ,  "connectAdaptorSource");
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return kafkaConnectSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };

    }

    private ConnectAdaptorSource<RowData> createConnectAdaptorSource(
            DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo
    ) {
        //need to wire up the properties
        ConnectAdaptorSource<RowData> source =
                new ConnectAdaptorSourceBuilder<RowData>(configs).
                        setDeserializer(new DynamicConnectAdaptorDeserializationSchema(keyDeserialization,
                                valueDeserialization,configs.get("value.converter")
                        )).outputToKafka(false).
                        setBounded(Boundedness.BOUNDED).build();

        return source;
    }
}
