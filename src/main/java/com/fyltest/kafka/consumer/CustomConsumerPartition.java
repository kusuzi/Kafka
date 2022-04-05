package com.fyltest.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author FYL
 * @date 2022/04/01 17:59
 * 消费者订阅first主题的0号分区
 **/
public class CustomConsumerPartition {
    public static void main(String[] args) {
        Properties props = new Properties();
        /**
         * 配置消费者的参数
         */
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        /**
         * 配置消费者组id
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        /**
         * 创建消费者对象
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        /**
         * 消费某个主题的某个分区数据
         */
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("first",0));
        kafkaConsumer.assign(topicPartitions);

        /**
         * 拉取数据并打印
         */
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

        }
    }
}
