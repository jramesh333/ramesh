package com.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaMessageConsumer {
	public int sleepIntervalms = 1000;

	@KafkaListener(topics = "${app.kafka.topic}", groupId = "my-group")
	public void listen(String message) {
		try {
			Thread.sleep(sleepIntervalms);
		} catch (Exception e) {
			log.info("Exception" + e.getMessage());
		}
		log.info("Consumed: " + message);
	}
}
