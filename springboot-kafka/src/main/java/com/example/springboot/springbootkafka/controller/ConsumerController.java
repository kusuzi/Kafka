package com.example.springboot.springbootkafka.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author FYL
 * @date 2022/04/03 15:27
 **/
@Configuration
public class ConsumerController {

    @KafkaListener(topics = "first")
    public void consumer(String msg){
        System.out.println("收到的消息:"+msg);


    }
}
