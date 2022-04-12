package com.tiankx.flinksql.service.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/12 9:24
 */
public class CassandraSinkFunction extends RichSinkFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final String insertQuery;
    private final ClusterBuilder builder;

    private transient Cluster cluster;
    private transient Session session;
    private transient PreparedStatement stmt;
    private transient FutureCallback<ResultSet> callback;
    private transient Throwable exception = null;
    private transient BatchStatement batchStatement;
    private transient Counter totalCounter;
    private transient Counter batchCounter;

    private final String[] fieldDataTypes;

    public CassandraSinkFunction(Properties prop) {
        this.fieldDataTypes = prop.getProperty("types").split("@");
        this.insertQuery = prop.getProperty("insert_cassandra_sql");
        this.builder = new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder
                        .addContactPoints(prop.getProperty("hosts"))
                        .withPort(Integer.parseInt(prop.getProperty("port")))
                        .withCredentials(prop.getProperty("username"), prop.getProperty("password"))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build()
                        ;
            }
        };
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.cluster = builder.getCluster();
        this.session = cluster.connect();
        this.stmt = session.prepare(insertQuery);
        this.batchStatement = new BatchStatement();
        this.callback = new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet rows) {
                onWriteSuccess(rows);
            }

            @Override
            public void onFailure(Throwable throwable) {
                onWriteFailure(throwable);
            }
        };
        this.totalCounter = getRuntimeContext().getMetricGroup().counter("cassandra_write_total_count");
        this.batchCounter = getRuntimeContext().getMetricGroup().counter("batch_now_count");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        super.invoke(value, context);
        Object[] fields = extractFields(value);
        ResultSetFuture result = session.executeAsync(stmt.bind(fields));
        Futures.addCallback(result, callback);
        totalCounter.inc();
    }

    protected Object[] extractFields(RowData record) {
        Object[] fields = new Object[record.getArity()];
        for (int i = 0; i < fields.length; i++) {
            String fieldDataType = fieldDataTypes[i].toLowerCase();
            switch (fieldDataType) {
                case "string":
                    fields[i] = record.getString(i).toString();
                    break;
                case "int":
                    fields[i] = record.getInt(i);
                    break;
                case "double":
                    fields[i] = record.getDouble(i);
                    break;
                case "bigint":
                    fields[i] = record.getLong(i);
                    break;
                case "array<string>": // TODO: type map<>
                    String arrStr = record.getString(i).toString();
                    if (arrStr != null) {
                        String[] split = arrStr.split(",");
                        fields[i] = Arrays.asList(split);
                    } else {
                        ArrayList<String> arrayList = new ArrayList();
                        fields[i] = arrayList;
                    }
                    break;
            }
            if (fieldDataType.startsWith("decimal")) {
                StringData decimalStr = record.getString(i);
                fields[i] = decimalStr;
            }
        }
        return fields;
    }

    protected void onWriteFailure(Throwable throwable) {
        exception = throwable;
    }

    protected void onWriteSuccess(ResultSet rows) {
    }
}
