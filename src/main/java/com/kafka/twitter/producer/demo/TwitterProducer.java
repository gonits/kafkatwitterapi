package com.kafka.twitter.producer.demo;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.Client;

public class TwitterProducer {
	private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	public static KafkaProducer<String, String> getNewKafkaProducer() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		return kafkaProducer;

	}

	public static void sendTweetsToKafka(Client hosebirdClient, KafkaProducer<String, String> kafkaProducer,
			BlockingQueue<String> msgQueue) {
		while (!hosebirdClient.isDone()) {
			String tweet = null;
			try {
				tweet = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("Exception occurred", e);
				hosebirdClient.stop();
			}
			if (tweet != null) {
				System.out.println("tweet is:" + tweet);
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("twitter_new", null, tweet);
				kafkaProducer.send(record, new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							logger.error("Exception occurred", exception);
						}
					}
				});
			}

		}
	}

}
