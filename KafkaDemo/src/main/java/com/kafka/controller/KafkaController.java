package com.kafka.controller;

import com.kafka.service.KafkaMessageConsumer;
import com.kafka.service.KafkaMessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Iterator;
import java.util.Set;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    @Autowired
    private KafkaMessageConsumer kafkaMessageConsumer;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final KafkaMessageProducer producer;
    public KafkaController(KafkaMessageProducer producer) {
        this.producer = producer;
    }

    @GetMapping("/stopConsumer/{consumer}")
    public String stopConsumer(@PathVariable("consumer") String  consumer) {
            MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumer);
            if (listenerContainer != null && listenerContainer.isRunning()) {
                listenerContainer.stop();
                System.out.println("Consumer with ID: " + consumer + " stopped.");
            } else {
                System.out.println("Consumer with ID: " + consumer + " is already stopped or doesn't exist.");
            }

        return "Stopped consumer ";
    }

    @GetMapping("/publish")
    public String publish() {
        producer.sendRandomMessage();
        return "Message Published!";
    }
    @GetMapping("/sleepInterval/{interval}")
    public String getSleepInterval(@PathVariable("interval") int interval) {
        kafkaMessageConsumer.sleepIntervalms = interval;
        return "Sleep interval is: " + interval;
    }

    @GetMapping("/status")
    public String status() {
        Set<String> listenerContainerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();
        StringBuilder sb = new StringBuilder();
        for (String listenerContainerId : listenerContainerIds) {
            sb.append(listenerContainerId);
       }
        return "Listener Container ids " + sb.toString();

    }

}
