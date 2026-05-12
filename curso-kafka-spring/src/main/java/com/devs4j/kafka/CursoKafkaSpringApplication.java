package com.devs4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class CursoKafkaSpringApplication {
	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);
	
	@KafkaListener(topics = "devs4j-topic", groupId = "devs4j-group")
	public void listen(String message) {
		log.info("Message received {}" ,message);
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

}
