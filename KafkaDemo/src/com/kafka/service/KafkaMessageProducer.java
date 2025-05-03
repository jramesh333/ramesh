package com.kafka.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendRandomMessage() {
        for (int i = 1; i <= 1000; i++) {
            String message = "Random Message " + i;
            kafkaTemplate.send(topic, message);
            System.out.println("Produced: " + message);
        }
    }


}
