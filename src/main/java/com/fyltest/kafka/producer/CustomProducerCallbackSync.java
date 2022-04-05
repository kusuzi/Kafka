package com.fyltest.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author QUEEN
 * @Classname CustomProducer
 * @Description TODO
 * @Date 2022/3/15 11:10
 * @Created by QUEEN
 * 【默认分区器+同步回调函数】
 */
public class CustomProducerCallbackSync {
    public static void main(String[] args) throws Exception {
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
            kafkaProducer.send(new ProducerRecord<>("first", "hi~" + i), new Callback() {
                // 该方法在 Producer 收到 ack 时调用，为异步调用[producer会一直发送，不会等到接受到ack才发送下一个]
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        //没有异常，输出信息到控制台
                        System.out.println("主题为："+metadata.topic()+"------>分区："+metadata.partition());
                    }else{
                        exception.printStackTrace();
                    }
                }
            }).get();
            // 延迟一会看到消息发送到分区
            Thread.sleep(50);
        }

        /**
         * 关闭资源
         */
        kafkaProducer.close();
    }
}
