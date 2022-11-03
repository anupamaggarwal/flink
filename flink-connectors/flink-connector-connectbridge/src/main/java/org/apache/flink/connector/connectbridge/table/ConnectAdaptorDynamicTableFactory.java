package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.connectbridge.table.ConnectAdaptorOptions.KEY_FORMAT;
import static org.apache.flink.connector.connectbridge.table.ConnectAdaptorOptions.VALUE_FORMAT;


/**
 * todo This will need a set of required options and optional options per connector
 * Maybe we can delegate to connecot validate for figuring these out
 * <p/> There might be some complexity introduced as a result of this
 */
public class ConnectAdaptorDynamicTableFactory implements DynamicTableSourceFactory {


    public static final String IDENTIFIER = "connectadaptor";


    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            FactoryUtil.TableFactoryHelper helper
    ) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(
                format -> {
                    if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                        throw new ValidationException(
                                String.format(
                                        "A key format should only deal with INSERT-only records. "
                                                + "But %s has a changelog mode of %s.",
                                        helper.getOptions().get(KEY_FORMAT),
                                        format.getChangelogMode()
                                ));
                    }
                });
        return keyDecodingFormat;
    }


    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            FactoryUtil.TableFactoryHelper helper
    ) {
        return helper.discoverOptionalDecodingFormat(
                        DeserializationFormatFactory.class, FactoryUtil.FORMAT)
                .orElseGet(
                        () ->
                                helper.discoverDecodingFormat(
                                        DeserializationFormatFactory.class, VALUE_FORMAT));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(
                this,
                context
        );

        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat =
                getKeyDecodingFormat(helper);

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);


        final ReadableConfig tableOptions = helper.getOptions();
        //skip all validations for now but here is where we would need to add validations shortly

        Map<String,String> connectorConfigs = context.getCatalogTable().getOptions();


        final DataType physicalDataType = context.getPhysicalRowDataType();


        return new ConnectAdaptorDynamicTableSource(
                connectorConfigs,
                physicalDataType,
                keyDecodingFormat.orElse(null),
                valueDecodingFormat,
                context.getObjectIdentifier().asSummaryString()
        );
    }

    private Map<String, String> getConnectorConfigs(ReadableConfig tableOptions) {



        return null;
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
