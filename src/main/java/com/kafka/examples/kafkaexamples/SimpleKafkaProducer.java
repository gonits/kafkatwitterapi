package com.kafka.examples.kafkaexamples;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleKafkaProducer {
	Properties properties = new Properties();
	Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

	public SimpleKafkaProducer() {
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	}

	public void produceMessages() throws InterruptedException, ExecutionException {
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("new_topic", String.valueOf(i),
					"Hello Again" + i);

			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("Topic is" + metadata.topic());
						logger.info("Partition is" + metadata.partition());
						logger.info("Partition is" + metadata.offset());
					} else {
						logger.error("Exception occurred", exception);
					}
				}
			}).get();
		}

		producer.flush();
		producer.close();
	}

}
