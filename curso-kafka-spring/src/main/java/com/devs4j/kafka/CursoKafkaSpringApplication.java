package com.devs4j.kafka;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(id = "devs4jId", autoStartup = "false", topics = "devs4j-topic", containerFactory = "listenerContainerFactory", groupId = "devs4j-group", properties = {
			"max.poll.interval.ms:4000", "max.poll.records:10" })
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Batch started to read messajes");

		for (ConsumerRecord<String, String> message : messages) {
			log.info("Offset {} Partition= {} Key = {} Value = {}", message.offset(), message.partition(),
					message.key(), message.value());
		}

		log.info("Batch Completed to read messajes");
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i),
					String.format("Enviando un mensaje desde Spring framework %d", i));
		}
		
		Thread.sleep(5000);
		registry.getListenerContainer("devs4jId").start();
	}

}
