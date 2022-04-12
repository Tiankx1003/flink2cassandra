package com.tiankx.flinksql.service.cassandra;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/12 9:04
 */
public class CassandraDynamicTableSyncFactory implements DynamicTableSinkFactory {
    Logger LOG = LoggerFactory.getLogger(CassandraDynamicTableSyncFactory.class);

    public static final String IDENTIFIER = "cassandra";
    public static final ConfigOption<String> TABLE_NAME = ConfigOptions.key("table_name").stringType().noDefaultValue().withDescription("table_name.");
    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue().withDescription("username.");
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue().withDescription("password.");
    public static final ConfigOption<String> KEY_SPACE = ConfigOptions.key("key_space").stringType().noDefaultValue().withDescription("cassandra db.");
    public static final ConfigOption<String> HOSTS = ConfigOptions.key("hosts").stringType().noDefaultValue().withDescription("cassandra conn hosts, split with: ,");
    public static final ConfigOption<String> PORT = ConfigOptions.key("port").stringType().noDefaultValue().withDescription("cassandra conn port.");
    public static final ConfigOption<String> PARSE_FORMAT = ConfigOptions.key("format").stringType().noDefaultValue().withDescription("json.");
    public static final ConfigOption<String> FIELD_TYPES = ConfigOptions.key("types").stringType().noDefaultValue().withDescription("json.");
    public static final ConfigOption<String> FIELD_NAMES = ConfigOptions.key("cols").stringType().noDefaultValue().withDescription("json.");
    public static final ConfigOption<String> INSERT_CASSANDRA_SQL = ConfigOptions.key("insert_cassandra_sql").stringType().noDefaultValue().withDescription("json.");
    public static final ConfigOption<String> SINK_PARALLELISM = ConfigOptions.key("sink_parallelism").stringType().noDefaultValue().withDescription("sink_parallelism.");

    private Properties parseConfig(ReadableConfig config) {
        Properties prop = new Properties();
        prop.setProperty(HOSTS.key(), config.get(HOSTS));
        prop.setProperty(PORT.key(), config.get(PORT));
        prop.setProperty(TABLE_NAME.key(), config.get(TABLE_NAME));
        prop.setProperty(USERNAME.key(), config.get(USERNAME));
        prop.setProperty(PASSWORD.key(), config.get(PASSWORD));
        prop.setProperty(PARSE_FORMAT.key(), config.get(PARSE_FORMAT));
        prop.setProperty(KEY_SPACE.key(), config.get(KEY_SPACE));
        prop.setProperty(FIELD_NAMES.key(), config.get(FIELD_NAMES));
        prop.setProperty(FIELD_TYPES.key(), config.get(FIELD_TYPES));
        prop.setProperty(INSERT_CASSANDRA_SQL.key(), config.get(INSERT_CASSANDRA_SQL));
        prop.setProperty(SINK_PARALLELISM.key(), config.get(SINK_PARALLELISM));
        return prop;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        LOG.info("create cassandra sink table!");
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final EncodingFormat<SerializationSchema<RowData>> format = helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        return new CassandraDynamicTableSink(parseConfig(config));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTS);
        requiredOptions.add(PORT);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        requiredOptions.add(PARSE_FORMAT);
        requiredOptions.add(KEY_SPACE);
        requiredOptions.add(FIELD_NAMES);
        requiredOptions.add(FIELD_TYPES);
        requiredOptions.add(INSERT_CASSANDRA_SQL);
        requiredOptions.add(SINK_PARALLELISM);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
