package com.tiankx.flinksql.exe;

import com.tiankx.flinksql.manager.FlinkJobHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/9 21:16
 */
public class KafkaHqlEntrence {
    private static Logger LOG = LoggerFactory.getLogger(KafkaHqlEntrence.class);
    private Properties prop;

    public KafkaHqlEntrence() throws IOException {
        prop = new Properties();
        prop.load(new InputStreamReader(ClassLoader.getSystemResourceAsStream("common.properties"), "utf-8"));
    }

    public static void main(String[] args) throws IOException {
        KafkaHqlEntrence kafkaHqlEntrence = new KafkaHqlEntrence();
        kafkaHqlEntrence.start();
    }

    public void start() {
        prop.put("group.id", prop.getProperty("groupid"));
        prop.put("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singletonList(prop.getProperty("reviceTopic")));
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        LOG.info("start deal topic, with prop: " + prop.toString());
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(1000);
            consumer.commitSync();
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> map = null;
                try {
                    map = objectMapper.readValue(value, Map.class);
                } catch (IOException e) {
                    LOG.error("error while deal record: [{}], error mesg: [{}]", value, e.toString());
                }
                Object o = map.get("t");
                if(o instanceof String){
                    String type = (String) o;
                    long ct = Long.parseLong((String) map.get("ct"));
                    if(prop.getProperty("flinkHQLMissionTittle").equals(type)){
                        LOG.info("raw msg: [{}]", value);
                        HashMap<String, Object> infoMap = (HashMap<String, Object>) map.get("v");
                        infoMap.put("recvice_date", System.currentTimeMillis() + "");
                        FlinkJobHandler flinkJobHandler = new FlinkJobHandler(infoMap, prop);
                        LOG.info("start deal record: [{}]", infoMap);
                        executorService.submit(flinkJobHandler);
                    }
                }
            }
        }
    }
}
