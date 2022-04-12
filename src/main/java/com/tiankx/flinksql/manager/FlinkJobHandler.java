package com.tiankx.flinksql.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tiankx.flinksql.utils.CommonUtils;
import com.tiankx.flinksql.utils.KafkaClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/9 21:41
 */
public class FlinkJobHandler implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(FlinkJobHandler.class);
    private HashMap<String, Object> infoMap;
    private Properties prop;

    public FlinkJobHandler(HashMap<String, Object> infoMap, Properties prop) {
        this.infoMap = infoMap;
        this.prop = prop;
    }

    @Override
    public void run() {
        int result = 1;
        StringBuilder errorSB = new StringBuilder();
        String realTableName = (String) infoMap.get("table_name");
        String tmpTableName = realTableName + "_flink_hql_tmp_table";
        String cassandra_env = (String) infoMap.get("cassandra_env");

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder
                .append("drop table tmp.")
                .append(tmpTableName)
                .append(";")
                .append("create table tmp.")
                .append(tmpTableName)
                .append(" stored as ORCFile as ")
                .append(infoMap.get("exe_sql").toString());

        try {
            String sqlFileStr = tmpTableName + ".sql";
            String sqlFilePathStr = prop.get("data_path") + sqlFileStr;

            int writeFileResult = CommonUtils.writeFile(sqlFilePathStr, sqlBuilder.toString());
            if (writeFileResult != 0) {
                sendResultMessageToKafka(writeFileResult, "write file failed");
                return;
            }
            int hiveSqlExecResult = CommonUtils.exeCmd(prop.getProperty("hive_sql_exec_shell"), sqlFilePathStr);
            if (writeFileResult != 0) {
                sendResultMessageToKafka(hiveSqlExecResult, "write file failed");
                return;
            }
        } catch (Exception e) {
            sendResultMessageToKafka(1, "deal hive sql failed");
        }

        String importSqlStr = "select * from tmp." + tmpTableName;
        infoMap.put("sql", importSqlStr);
        infoMap.put("format", "json");
        infoMap.put("username", prop.getProperty("cassandra_username"));
        infoMap.put("password", prop.getProperty("cassandra_password"));
        infoMap.put("host", prop.getProperty("cassandra_host"));
        infoMap.put("port", prop.getProperty("cassandra_port"));
        infoMap.put("hive_conf_path", "json");
        if (!infoMap.containsKey("sink_parallelism")) {
            infoMap.put("sink_parallelism", "5");
        }

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String params = gson.toJson(infoMap);
        LOG.info("input param: " + params);

        List<String> execCmd = getExecCmd(params);

        ProcessBuilder hiveProcessBuilder = new ProcessBuilder(execCmd);
        Process start = null;

        try {
            start = hiveProcessBuilder.start();
            InputStream inputStream = start.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            StringBuilder infoSB = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                LOG.info(line);
                infoSB.append(line);
            }
            InputStream errorStream = start.getErrorStream();
            BufferedReader errorBufferReader = new BufferedReader(new InputStreamReader(errorStream));
            while ((line = errorBufferReader.readLine()) != null) {
                LOG.info(line);
                errorSB.append(line);
            }
            result = start.waitFor();
            LOG.info("job exec result: {}", result);
            sendResultMessageToKafka(result, errorSB.toString());
        } catch (Exception e) {
            sendResultMessageToKafka(1, "import job failed");
        }
    }

    /**
     * 回传消息
     *
     * @param result 消息状态标记，0正常
     * @param msg    报错内容，正常时为空
     */
    private void sendResultMessageToKafka(int result, String msg) {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("unit", infoMap.get("unit"));
        resultMap.put("unique_mark", infoMap.get("unique_mark"));
        resultMap.put("data_time", infoMap.get("data_time"));
        resultMap.put("deal_status", result + "");
        if (result == 0) {
            msg = "";
        }
        resultMap.put("error_massage", msg);

        HashMap<String, Object> postBack = new HashMap<>();
        postBack.put("v", resultMap);
        postBack.put("t", prop.getProperty("flinkHQLMissionResultTile"));
        postBack.put("ct", System.currentTimeMillis());
        postBack.put("id", infoMap.get("unique_mark"));

        try {
            KafkaClientUtil.sendMsg(gson.toJson(postBack),
                    prop.getProperty("bootstrap.servers"),
                    prop.getProperty("sendTopic"));
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 拼接参数到flink run命令
     *
     * @param params 命令参数
     * @return flink run 语句
     */
    public List<String> getExecCmd(String params) {
        ArrayList<String> cmd = new ArrayList<>();
        cmd.add(prop.getProperty("flink_exe"));
        cmd.add("run");
        cmd.add("-t");
        cmd.add("yarn-per-job");
        cmd.add("-Dyarn.application.name=" + infoMap.get("key_space") + "." + infoMap.get("table_name"));
        cmd.add("-c");
        cmd.add("com.tiankx.flink.hql.exe.FlinkHqlToCassandra");
        cmd.add(prop.getProperty("jar_path"));
        cmd.add(params);
        return cmd;
    }
}
