package com.fyltest.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author FYL
 * @date 2022/04/01 17:33
 * 独立消费者消费first主题中的数据,不订阅分区
 **/
public class CustomConsumer {

    public static void main(String[] args) {
        /**
         * 配置消费者的相关参数
         */
       Properties props = new Properties();
       props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
       props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /**
         * 必须配置消费者组（一个消费者也需要）
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test");


        /**
         * 创建消费者对象
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        /**
         * 订阅需要消费的主题（可以是多个主题）
         */
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);


        /**
         * 拉取数据并打印
         * 设置 1s 中消费一批数据
         */
        while (true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }


    }
}
