package com.fyltest.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
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
 * 1. 指定特定的分区进行数据的发送
 * 2. partition为空，key不为空
 * 3. 自定义分区
 */
public class CustomProducerCallbackPartitions {
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
         * 添加自定义分区器的全类名
         */
//        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.fyltest.kafka.producer.MyPartitioner");

        /**
         * 创建生产者
         */
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        /**
         * 发送消息
         */
        for (int i = 0; i < 5; i++) {
            // 该方法在 Producer 收到 ack 时调用，为异步调用[producer会一直发送，不会等到接受到ack才发送下一个]
            Callback callback = ( metadata,  exception)->{
                if(exception==null){
                    //没有异常，输出信息到控制台
                    System.out.println("主题为："+metadata.topic()+"------>分区："+metadata.partition());
                }else{
                    exception.printStackTrace();
                }
            };

//            (1)指定数据发送到 1 号分区，key 为空
              kafkaProducer.send(new ProducerRecord<>("first", 0, "", "Hi~ " + i),callback);

//             (2)partition为空，key不为空,分别为a/b/c,对应的分区分别为1/2/1
//              kafkaProducer.send(new ProducerRecord<>("first", "c", "Hi~ " + i),callback);
//             (3) 自定义分区
//              kafkaProducer.send(new ProducerRecord<>("first", "fyl" + i),callback);


            // 延迟一会看到消息发送到分区
            Thread.sleep(2);
        }

        /**
         * 关闭资源
         */
        kafkaProducer.close();
    }
}
