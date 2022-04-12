package com.tiankx.flinksql.utils;

import com.tiankx.flinksql.exe.FlinkSQL2Cassandra;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/3/16 9:34
 */
public class CommonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

    public static Map<String, String> parseArgs(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(args[0], Map.class);
    }

    public static String concatInsertSql(TableSchema tmpSchema, Map<String, String> paramMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into sinkCassandraTable \n");
        sb.append("select ");

        String[] fieldNames = tmpSchema.getFieldNames();
        DataType[] fieldDataTypes = tmpSchema.getFieldDataTypes();

        sb.append(fieldNames[0]);
        for (int i = 1; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            String type = fieldDataTypes[i].getLogicalType().asSummaryString().toLowerCase();
            if (type.equals("array<string>")) {
                sb.append("," + "arr_to_str(" + fieldName + ") as " + fieldName);
            } else if (type.startsWith("decimal") || type.startsWith("date")) {
                sb.append("," + "cast(" + fieldName + ") as string  as " + fieldName);
            } else {
                if (!fieldName.equals("position")) { // key&reserve words
                    sb.append("," + fieldName);
                } else {
                    sb.append(",`" + fieldName + "`");
                }
            }
        }
        sb.append(" from " + FlinkSQL2Cassandra.HIVE_TMP_TABLE_NAME);
        return sb.toString();
    }

    public static String concatSchema(TableSchema tmpSchema, Map<String, String> paramMap) {
        StringBuilder sb = new StringBuilder();
        StringBuilder fieldNameStr = new StringBuilder();
        StringBuilder fieldTypes = new StringBuilder();
        StringBuilder insertCassandraSql = new StringBuilder();
        StringBuilder sqlMatch = new StringBuilder();
        StringBuilder cassandraTableInitSql = new StringBuilder();

        insertCassandraSql
                .append("INSERT INTO ")
                .append("key_space" + ".")
                .append("table_name" + " (");

        sqlMatch.append("(");
        sb.append("create table sindCassandraTable \n");

        cassandraTableInitSql
                .append("CREATE TABLE ")
                .append(paramMap.get("key_space"))
                .append(".")
                .append(paramMap.get("table_name"))
                .append(" (");

        String[] fieldNames = tmpSchema.getFieldNames();
        DataType[] fieldDataTypes = tmpSchema.getFieldDataTypes();

        for (int i = 0; i < fieldDataTypes.length; i++) {
            DataType fieldDataType = fieldDataTypes[i];
            LogicalType logicalType = fieldDataType.getLogicalType();
            String s = logicalType.asSummaryString().toLowerCase();
            String fieldName = fieldNames[i].toLowerCase();
            if (i != fieldDataTypes.length - 1) {
                if (s.startsWith("array") || s.startsWith("decimal") || s.equals("date")) {
                    s = "string";
                }
                if (!fieldName.equals("position")) {
                    sb.append(fieldName + " " + s + ",\n");
                } else {
                    sb.append("`" + fieldName + "` " + s + ",\n");
                }
                fieldTypes.append(s + "@");
                fieldNameStr.append(fieldName + ",");
                insertCassandraSql.append(fieldName + ",");
                sqlMatch.append("?,");
            } else {
                sb.append(fieldName + " " + s + "\n");
                fieldTypes.append(s);
                fieldNameStr.append(fieldName);
                insertCassandraSql.append(fieldName + ") values");
                sqlMatch.append("?)");
            }

            // Cassandra type mapping processing: array -> list, string -> text, decimal -> text, date -> text
            if (s.contains("decimal") || s.equals("date") || s.equals("string")) {
                cassandraTableInitSql.append(fieldName).append(" ").append("text").append(",");
            } else if (s.equals("array<string")) {
                cassandraTableInitSql.append(fieldName).append(" ").append("list<text>").append(",");
            } else {
                cassandraTableInitSql.append(fieldName).append(" ").append(s).append(",");
            }
        }
        insertCassandraSql.append(sqlMatch);
        cassandraTableInitSql.append("PRIMARY KEY(")
                .append(paramMap.get("primary_key"))
                .append(" ));");

        paramMap.put("cassandraTableInitSql", cassandraTableInitSql.toString());
        paramMap.put("deleteTableSql",
                "DROP TABLE IF EXISTS " + paramMap.get("key_space") + "." + paramMap.get("table_name"));

        sb.append(")with (\n");
        sb.append("'connector' = 'cassandra', \n");
        sb.append("'host' = '" + paramMap.get("hosts") + "', \n");
        sb.append("'port' = '" + paramMap.get("port") + "', \n");
        sb.append("'key_space' = '" + paramMap.get("key_space") + "', \n");
        sb.append("'table_name' = '" + paramMap.get("table_name") + "', \n");
        sb.append("'user_name' = '" + paramMap.get("user_name") + "', \n");
        sb.append("'password' = '" + paramMap.get("password") + "', \n");
        sb.append("'format' = '" + paramMap.get("format") + "', \n");
        sb.append("'sink_parallelism' = '" + paramMap.get("sink_parallelism") + "', \n");
        sb.append("'cols' = '" + fieldNameStr.toString().toLowerCase() + "', \n");
        sb.append("'types' = '" + fieldTypes.toString().toLowerCase() + "', \n");
        sb.append("'insert_cassandra_sql' = '" + insertCassandraSql.toString() + "'\n");
        sb.append(")");

        return sb.toString();
    }

    /**
     * 写入文本内容到文件
     *
     * @param pathStr 文件路径
     * @param content 文本内容
     * @return 返回状态
     */
    public static int writeFile(String pathStr, String content) {
        try {
            File file = new File(pathStr);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(content);
            fileWriter.flush();
            fileWriter.close();
        } catch (Exception e) {
            return 1;
        }
        return 0;
    }

    public static int exeCmd(String cmd, String param) throws IOException, InterruptedException {
        ArrayList<String> command = new ArrayList<String>();
        command.add(cmd);
        command.add(param);

        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);
        Process start = hiveProcessBuilder.start();
        InputStream inputStream = start.getInputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            LOG.info("std out: " + line);
        }
        InputStream errorStream = start.getErrorStream();
        BufferedReader errorBufferReader = new BufferedReader(new InputStreamReader(errorStream));
        while ((line = errorBufferReader.readLine()) != null) {
            LOG.error("error: " + line);
        }
        int result = start.waitFor();
        LOG.info("hive sql exec result: " + result);
        return result;
    }
}
