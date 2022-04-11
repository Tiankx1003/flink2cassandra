package com.tiankx.flinksql.utils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/3/16 10:02
 */
public class CassandraClientUtil {
    private static Logger LOG = LoggerFactory.getLogger(CassandraClientUtil.class);
    public static boolean initTable(Map<String, String> paramMap){
        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint(paramMap.get("host"))
                        .withPort(Integer.parseInt(paramMap.get("port")))
                        .withCredentials(paramMap.get("username"), paramMap.get("password"))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
            }
        };
        Cluster cluster = clusterBuilder.getCluster();
        Session session = cluster.connect();

        String deleteTableSql = paramMap.get("deleteTableSql");
        LOG.info("exec cassandra table delete sql: " + deleteTableSql);
        session.execute(deleteTableSql);

        String cassandraTableInitSql = paramMap.get("cassandraTableInitSql");
        LOG.info("exec cassandra table create sql: " + cassandraTableInitSql);
        session.execute(cassandraTableInitSql);

        return true;
    }
}
