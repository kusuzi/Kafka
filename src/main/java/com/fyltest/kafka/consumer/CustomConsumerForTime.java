package com.fyltest.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author FYL
 * @date 2022/04/02 11:19
 * 对分区中的数据从指定的时间开始消费
 **/
public class CustomConsumerForTime {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test3");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        ArrayList<String> topic = new ArrayList<>();
        topic.add("first");
        kafkaConsumer.subscribe(topic);
        
        /**
         * 获取分区策略
         */
        Set<TopicPartition> assignment = new HashSet<>();
        while(assignment.size() ==0){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }
        /**
         * 将时间转换为offset
         */
        Map<TopicPartition, Long> topicPartions = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            topicPartions.put(topicPartition,System.currentTimeMillis()-1*24*3600*1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaConsumer.offsetsForTimes(topicPartions);
        /**
         * 对每个分区指定offset
         */
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            if(offsetAndTimestamp!=null){
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }

        /**
         * 消费数据
         */
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println(consumerRecord);
        }


    }
}
