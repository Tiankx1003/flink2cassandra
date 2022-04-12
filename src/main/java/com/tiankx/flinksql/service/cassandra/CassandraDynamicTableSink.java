package com.tiankx.flinksql.service.cassandra;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

import java.util.Properties;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/12 9:20
 */
public class CassandraDynamicTableSink implements DynamicTableSink {
    private final Properties properties;

    public CassandraDynamicTableSink(Properties prop) {
        this.properties = prop;
    }

    /**
     * 返回sink端的变化
     *
     * @param changelogMode
     * @return
     */
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        CassandraSinkFunction cassandraSinkFunction = new CassandraSinkFunction(properties);
        return SinkFunctionProvider.of(cassandraSinkFunction, Integer.parseInt(properties.getProperty("sink_parallelism")));
    }

    @Override
    public DynamicTableSink copy() {
        return new CassandraDynamicTableSink(properties);
    }

    @Override
    public String asSummaryString() {
        return "Cassandra Table Sink";
    }
}
