package com.tiankx.flinksql.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/4/9 22:47
 */
public class KafkaClientUtil {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientUtil.class);

    public static void sendMsg(String msg, String broker, String topic) throws ExecutionException, InterruptedException {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("request.required.acks", "1");
        prop.put("serializer.class", "kafka.serializer.DefaultEncoder");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bak.partitioner.class", "kafka.producer.DefaultPartitioner");
        prop.put("bak.key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("bak.value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(prop);
        Future<RecordMetadata> send = kp.send(new ProducerRecord<>(topic, null, msg));
        RecordMetadata recordMetadata = send.get();

        LOG.info("send message: " + msg);
    }
}
