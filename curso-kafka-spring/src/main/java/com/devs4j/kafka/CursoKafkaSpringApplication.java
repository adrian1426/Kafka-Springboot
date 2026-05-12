package com.devs4j.kafka;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(topics = "devs4j-topic", groupId = "devs4j-group")
	public void listen(String message) {
		log.info("Message received {}", message);
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic",
				"Enviando un mensaje desde Spring framework");

		future.whenComplete((result, ex) -> {
			if (ex == null) {
				// ÉXITO (onSuccess)
				log.info("Enviado con offset: {}" , result.getRecordMetadata().offset());
			} else {
				// ERROR (onFailure)
				// Si necesitas los detalles específicos de Kafka:
				if (ex instanceof KafkaProducerException kex) {
					System.err.println("Error en record: " + kex.getFailedProducerRecord());
				}
				System.err.println("Causa: " + ex.getMessage());
			}
		});
	}

}
