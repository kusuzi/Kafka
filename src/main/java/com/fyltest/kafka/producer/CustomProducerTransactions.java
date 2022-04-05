package com.fyltest.kafka.producer;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author QUEEN
 * @Classname CustomProducerTransactions
 * @Description TODO
 * @Date 2022/3/15 15:42
 * @Created by QUEEN
 * 在生产者端【开启事务】
 * 注意：需要指定全局的transaction_id
 * (1)初始化事务
 * (2)开启事务
 * (3)在事务内提交已经消费的偏移量
 * (4)提交事务
 * (5)事务回滚
 */
public class CustomProducerTransactions {
    public static void main(String[] args) {
        //1. 设置参数
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 设置全局的事务id
         */
        prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transaction_id_0");

        //2. 创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        /**
         * 初始化事务
         */
        kafkaProducer.initTransactions();
        /**
         * 开启事务
         */
        kafkaProducer.beginTransaction();

        //3. 发送消息
        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("first","hello!"+i));
            }
            int i=1/0;
            /**
             * 提交事务
             */
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            /**
             * 回滚事务
             */
            kafkaProducer.abortTransaction();
        } finally {
            //4. 关闭资源
            kafkaProducer.close();
        }
    }
}
