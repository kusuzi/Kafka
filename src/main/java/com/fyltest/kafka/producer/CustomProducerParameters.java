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
 * 【设置一些其他的参数，使得生产者提高吞吐量】
 * 有以下参数可以设置：
 * batchsize
 * lingerms
 * compression type
 * recordAccumulator
 *
 */
public class CustomProducerParameters {
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
         *  设置其他的参数
         *  batch.size：批次大小，默认 16K
         *  linger.ms：等待时间，默认 0ms
         *  RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
         *  compression.type：压缩，默认 none，可配置值 gzip、snappy、lz4 和 zstd
         */
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1);
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        prop.put(ProducerConfig.LINGER_MS_CONFIG,1);
        prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


        /**
         * 创建生产者
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        /**
         * 发送消息
         */
        for (int i = 0; i < 5; i++) {

            kafkaProducer.send(new ProducerRecord<>("first","hi~ "+i));
        }

        /**
         * 关闭资源
         */
        kafkaProducer.close();
    }
}
