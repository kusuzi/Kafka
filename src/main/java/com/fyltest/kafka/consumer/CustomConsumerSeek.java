package com.fyltest.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * @author FYL
 * @date 2022/04/02 10:36
 * first主题中某个分区指定从某个offset开始消费
 **/
public class CustomConsumerSeek {
    public static void main(String[] args) {
        /**
         * 0. 配置参数
         */
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /**
         * 消费者组id
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");

        /**
         * 1. 创建消费者对象
         */
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        /**
         * 2. 消费者订阅主题
         */
        ArrayList<String> topic = new ArrayList<>();
        topic.add("first");
        kafkaConsumer.subscribe(topic);
        /**
         * 3. 指定分区的offset
         */
        Set<TopicPartition> assignment = new HashSet<>();
        //因为制定分区策略需要时间，所以需要等待
        while (assignment.size() == 0) {
            // 能够加快制定分区策略的进程
            kafkaConsumer.poll(Duration.ofSeconds(1));
            //获取分区信息
            assignment = kafkaConsumer.assignment();
        }

        // 对每个分区指定offset，从该位置开始消费
        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition, 20);
        }
        /**
         * 4. 从指定offset开始消费分区的数据
         */
        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }


    }
}
