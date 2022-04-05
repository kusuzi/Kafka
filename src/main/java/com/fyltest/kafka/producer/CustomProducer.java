package com.fyltest.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author QUEEN
 * @Classname CustomProducer
 * @Description TODO
 * @Date 2022/3/15 11:10
 * @Created by QUEEN
 * 最简单的生产者，没有过多的参数
 * 【默认分区器】
 */
public class CustomProducer {
    public static void main(String[] args) {
        /**
         * 连接服务器和设置参数
         */
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // 序列化【必需】key.serializer
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 创建生产者
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        /**
         * 发送消息
         */
        for (int i = 0; i < 5; i++) {

            kafkaProducer.send(new ProducerRecord<>("first","hi~"+i));
        }

        /**
         * 关闭资源
         */
        kafkaProducer.close();
    }
}
