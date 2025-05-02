package com.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageConsumer {
    public int sleepIntervalms = 1000;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "my-group")
    public void listen(String message) {
       try {
           Thread.sleep(sleepIntervalms);
       }catch (Exception e){
           System.out.println("Exception"+ e.getMessage());
       }
        System.out.println("Consumed: " + message);
    }
}
