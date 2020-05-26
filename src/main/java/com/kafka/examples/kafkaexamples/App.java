package com.kafka.examples.kafkaexamples;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {
		SimpleKafkaProducer prod = new SimpleKafkaProducer();
		SimpleKafkaConsumer cons = new SimpleKafkaConsumer();
		Thread t1 = new Thread(() -> {
			try {
				prod.produceMessages();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});
		Thread t2 = new Thread(() -> {
			cons.consumeMessages();

		});

		t1.start();
		t2.start();

		try {
			t1.join();
			t2.join();
		} catch (InterruptedException e) {
			logger.error("Exception occurred", e);
		}

	}
}
