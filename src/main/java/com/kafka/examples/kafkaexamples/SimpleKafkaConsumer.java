package com.kafka.examples.kafkaexamples;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleKafkaConsumer {
	Properties properties = new Properties();
	private int count = 0;
	Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

	public SimpleKafkaConsumer() {
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Second_application");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	public void consumeMessages() {
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
				properties);
		consumer.subscribe(Arrays.asList("new_topic"));
		boolean keepOnReading = true;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			consumer.close();
		}));
		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				count++;
				logger.info("consumer consumed record with key as:" + record.key() + "and value as " + record.value());
				logger.info("consumer consumed record from partion :" + record.partition() + "and offset is "
						+ record.offset());
				if (count == 10) {
					keepOnReading = false;
					break;
				}
			}
		}
		consumer.commitSync();

	}

}
