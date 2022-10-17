package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.connectbridge.table.ConnectAdaptorOptions.VALUE_FORMAT;


/**
 * todo This will need a set of required options and optional options per connector
 * Maybe we can delegate to connecot validate for figuring these out
 * <p/> There might be some complexity introduced as a result of this
 */
public class ConnectAdaptorTableFactory implements DynamicTableSourceFactory {


    public static final String IDENTIFIER = "connectadaptor";


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        /*final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        helper.validateExcept(PROPERTIES_PREFIX);

        final ReadableConfig tableOptions = helper.getOptions();

        validateTableSourceOptions(tableOptions);

        validatePKConstraints(
                context.getObjectIdentifier(),
                context.getPrimaryKeyIndexes(),
                context.getCatalogTable().getOptions(),
                valueDecodingFormat);

        final StartupOptions startupOptions = getStartupOptions(tableOptions);

        final Properties properties = getKafkaProperties(context.getCatalogTable().getOptions());

        // add topic-partition discovery
        final Optional<Long> partitionDiscoveryInterval =
                tableOptions.getOptional(SCAN_TOPIC_PARTITION_DISCOVERY).map(Duration::toMillis);
        properties.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                partitionDiscoveryInterval.orElse(-1L).toString());

        final DataType physicalDataType = context.getPhysicalRowDataType();*/

        return new ConnectAdaptorTableSource();
    }



    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }



    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();

        return options;
    }

}
