package com.example.springboot.springbootkafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author FYL
 * @date 2022/04/03 15:19
 * 从浏览器接收数据后，发送到相应的主题中
 **/
@RestController
public class ProducerController  {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/kafkasent")
    public String send(String msg){
        kafkaTemplate.send("first",msg);
        return "OK";
    }
}
