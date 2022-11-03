package org.apache.flink.connector.connectbridge.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

public class ConnectAdaptorOptions {


    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value key. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

}
