package com.tiankx.flinksql.exe;

import com.tiankx.flinksql.func.ArrayToStrFunction;
import com.tiankx.flinksql.utils.CassandraClientUtil;
import com.tiankx.flinksql.utils.CommonUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/3/16 9:33
 */
public class FlinkSQL2Cassandra {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQL2Cassandra.class);

    public static final String HIVE_TMP_TABLE_NAME = "tmp";
    private static final String HIVE_CATALOG = "tiankx";
    private static final String MEMORY_CATALOG = "memory";
    private static final String defaultDatabase = "default";
    private static final String name = "tiankx";

    /**
     * @param args json
     *             {
     *             "sql": "select t1.col_1, t1.col_2, t2.col_2 from tab_1 t1 join tab_2 t2 on t1.col_1 = t2.col_1",
     *             "hosts": "192.168.1.101",
     *             "port": "9042",
     *             "key_space": "test",
     *             "table_name":"test_tiankx_00",
     *             "username": "tiankx",
     *             "password": "tiankx",
     *             "format": "json",
     *             "primary_key": "userid",
     *             "sink_parallelism": "30",
     *             "data_time": "20220228",
     *             "hive_conf_path": "xxxx/flinksql2cassandra/hive_conf"
     *             }
     * @throws IOException          args parse exception
     * @throws InterruptedException sleep interrupted
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> paramMap = CommonUtils.parseArgs(args);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporarySystemFunction("arr_to_str", ArrayToStrFunction.class);

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, paramMap.get("hive_conf_path"));
        tableEnv.registerCatalog(HIVE_CATALOG, hive);
        tableEnv.useCatalog(HIVE_CATALOG);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String sql = paramMap.get("sql");
        LOG.info("hive sql: " + sql);
        Table tmpTable = tableEnv.sqlQuery(sql); // return HiveSQL execution result
        TableSchema schema = tmpTable.getSchema();
        String insertSql = CommonUtils.concatInsertSql(schema, paramMap);
        String registSql = CommonUtils.concatSchema(schema, paramMap);
        LOG.info("get input param: {}\n\nparse insert sql: {}\n\nparseregist sql: {}", paramMap, insertSql, registSql);

        CassandraClientUtil.initTable(paramMap);

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.registerCatalog(MEMORY_CATALOG, new GenericInMemoryCatalog(MEMORY_CATALOG));
        tableEnv.useCatalog(MEMORY_CATALOG);

        tableEnv.createTemporaryView(HIVE_TMP_TABLE_NAME, tmpTable);
        tableEnv.executeSql(registSql);
        TableResult tableResult = tableEnv.executeSql(insertSql);

        JobClient jobClient = tableResult.getJobClient().get();
        CompletableFuture<JobExecutionResult> jobExecutionResultFuture = jobClient.getJobExecutionResult();

        while (!jobExecutionResultFuture.isDone()) {
            Thread.sleep(20000);
            LOG.info("sleep 20 second wait job finish");
        }

        boolean done = jobExecutionResultFuture.isDone();
        boolean cancelled = jobExecutionResultFuture.isCancelled();
        boolean completedExceptionally = jobExecutionResultFuture.isCompletedExceptionally();

        LOG.info("job exec result: " + "[isDone:{}],[isCancelled:{}],[isCompletedExecptionally:{}]",
                done, cancelled, completedExceptionally);

        if (completedExceptionally) {
            throw new RuntimeException("job exec failed");
        } else if (cancelled) {
            throw new RuntimeException("job cancelled");
        }

        LOG.info("job finished SUCCESSFULL");
    }
}
